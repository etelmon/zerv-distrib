
const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');

const serverStatusService = require('./server-status.service');
const processService = require('./process.service');
const processMonitorClient = require('./process-monitor-client.service');
const queueLockService = require('./queue-lock.service');
const BasicCapacityProfile = require('./basic-capacity-profile');

const logger = zlog.getLogger('process-monitor/server');

const supportedProcessTypes = {};
let zerv;
let activeProcesses;
let waitForProcessingQueue;
let capacityProfile;
const thisServerStart = new Date();

const service = {
  setZervDependency,
  monitorQueue,
  addProcessType,
  getServerId: serverStatusService.getServerId,

  _rescheduledProcesses,
  _scheduleProcess,
  _runStalledRunningProcesses,
  _scheduleToCheckForNewProcessResquests,
  _selectNextProcessesToRun,
  _executeProcess,
  _setCapacityProfile
};

module.exports = service;

function setZervDependency(zervInstance) {
  zerv = zervInstance;
  activeProcesses = {};
  waitForProcessingQueue = null;
}

/**
 * add the type of process handled by the zerv server and its implementation
 * When process is submitted by the queue, this zerv instance will process it if capacity allows.
 * 
 * @param {String} type
 * @param {function} executeFn
 * @param {Object} options
 *   @property {Number} gracePeriodInMins       : if the process did not respond or complete in thist time, it might have crashed. so give a chance to restart it to this server
 *   @property {Boolean} autoRestartWhenStalled :  The process will restart if it is stalled, by default process will not auto restart.
 *   @property {Number} wasteTimeInSecs         : This will add a duration to a process execution useful to simulate/test more concurrency
 */
function addProcessType(type, executeFn, options = {}) {
  let execute;
  if (_.isNumber(options.wasteTimeInSecs) && options.wasteTimeInSecs>0) {
    // delay execution to pretend the process is slow.
    execute = function() {
      return new Promise((resolve, reject) => {
        setTimeout(async () => {
          try {
            resolve(await executeFn.apply(this, arguments));
          } catch (err) {
            reject(err);
          }
        }
        , options.wasteTimeInSecs * 1000);
      });
    };
  } else {
    execute = executeFn;
  }
  supportedProcessTypes[type] = _.assign({ execute }, options);
}


/**
 * This function starts the process queue monitor
 * Processes submitted in the queue will be executed by this server
 * only if it handles the type of process submitted and if it is not already running full capacity.
 *
 * This function also notifies the server status
 * @param {String} serverName  the name or type of server
 * @param {Number} appVersion which the port where it runs
 * @param {Number} capacityProfileConfig the number of processes the server can run at the same time
 */
function monitorQueue(serverName, appVersion, capacityProfileConfig) {
  if (capacityProfileConfig === 0) {
    capacityProfile = null;
  } else {
    service._setCapacityProfile(capacityProfileConfig);
  }

  // ZERV_STAY_ALIVE is the duration before server is no longer considered alive by the cluster
  serverStatusService.createOne(serverName, appVersion);
  // if there is no capacity or process type supported. monitor does NOT listen to the queue
  if (zerv.getRedisClient() && capacityProfile && _.keys(supportedProcessTypes).length) {
    listenProcessQueueForNewRequests();
  }
  serverStatusService.notifyServerStatusPeriodically();
}

/**
 * A config would provide some direction to initialize
 * the proper algorithm to handle server capacity
 * Currently config is just the max number of processed
 * which return a profile object that would restrict the concurrent number of processes.
 * and manage priorities on processed.
 *
 * but it would also be an instance extending a Capacity Profile Class
 * that could handle capacity in a different way.
 * Ex: not only number of processes but cpu historical activity
 * Ex: tenant restriction (tenant could have priority over others)
 * Ex: based on cluster load
 * ...
 *
 * @param {Object} config
 */
function _setCapacityProfile(config) {
  if (_.isNil(config) || _.isNumber(config)) {
    capacityProfile = new BasicCapacityProfile(
        Number(config || process.env.ZERV_MAX_PROCESS_CAPACITY || 30)
    );
    capacityProfile.setActiveProcesses(activeProcesses);
  } else {
    throw new Error('capacity profile config not supported. Only a the number of maximum processes is supported.');
  }
  return capacityProfile;
}

const scheduledProcess = {};

function listenProcessQueueForNewRequests() {
  logger.info('Server %b is monitoring process queue for executing processes %b. max capacity: %s', serverStatusService.getServerId(), _.keys(supportedProcessTypes), capacityProfile);

  zerv.onChanges('TENANT_PROCESS_DATA', async (tenantId, process, notificationType) => {
    if (process.status === processService.PROCESS_STATUSES.SCHEDULED) {
      logger.info('Received new process in the queue [%s/%s] scheduled to run on %s', process.type, process.name, moment(process.scheduledDate));
      service._scheduleProcess(process);
    } else if (process.status === processService.PROCESS_STATUSES.PENDING) {
      logger.info('Received new process in the queue: [%s/%s]', process.type, process.name);
      service._scheduleToCheckForNewProcessResquests(capacityProfile);
    }
  });

  setTimeout(() => {
    // when the server starts, it will try to run all process request in the queue up to its capacity;
    service._rescheduledProcesses();
    logger.info('Check and run pending processes in the queue as per capacity %b...', capacityProfile);
    service._scheduleToCheckForNewProcessResquests();

    // Safety Check: every now and then,let's recheck the queue.
    // If locking mechanism fails (redis failure for ie), there might be process requests in the queue
    // not handled by any server if there is no activity in the infrastructure.
    //
    setInterval(async () => {
      if (!capacityProfile.isServerAtFullCapacity()) {
        logger.debug('Process queue health check for unhandled process requests');
        await service._runStalledRunningProcesses();
      }
    }, (process.env.ZERV_PROCESS_QUEUE_HEALTH_CHECK_IN_SECS || 60)* 1000);
  }, 5 * 1000);
}


/**
 * When server starts, it will plan to run scheduled processes.
 *
 * All servers will attempt to run this scheduled process at the proper time
 * only one will succeed to run it.
 *
 */
async function _rescheduledProcesses() {
  logger.info('Scheduled processes in the queue...');
  const processes = await processService.findAllScheduledProcessesByTypes(_.keys(supportedProcessTypes));
  for (const process of processes) {
    await service._scheduleProcess(process);
  }
}

/**
 * Schedule to run the process
 * Or submit for execution now if delay is over.
 * @param {Object} process
 */
async function _scheduleProcess(process) {
  const delay = moment(process.scheduledDate).toDate().getTime() - new Date().getTime();
  if (delay<=0) {
    await runScheduledProcessNow(process);
  } else {
    scheduledProcess[process.id] = {
      process,
      timeout: setTimeout(() =>
        runScheduledProcessNow(process)
      , delay)
    };
  }

  async function runScheduledProcessNow(process) {
    delete scheduledProcess[process.id];
    let lock;
    try {
      // make sure another server is not trying to run this process at the same time
      lock = await queueLockService.getScheduleQueueLock();
      process = await processService.setScheduledProcessToPending(process);
    } catch (err) {
      logger.error('Error when scheduling process %s', err.message, err.stack || err);
      // this is a major issue. scheduling will not be taken in consideration
    } finally {
      if (lock) {
        lock.release();
      }
    }
    if (process) {
      service._scheduleToCheckForNewProcessResquests();
    }
  }
}

async function _runStalledRunningProcesses() {
  let lock;
  try {
    // Lock the progress queue before updating potentially stalled processes
    // since the queue is locked, no other server will be able to check for stalled process
    // which give time, to reset them to the proper state.
    lock = await queueLockService.getInProgressQueueLock();
    const processes = await processService.findAllInProgressProcessesByTypes(_.keys(supportedProcessTypes));
    for (const process of processes) {
      if (checkIfProcessIsStalled(process)) {
        if ( supportedProcessTypes[process.type].autoRestartWhenStalled === true) {
          // Risk: the process might have just completed, but then it is set back to pending
          await processService.setRunningProcessBackToPending(process);
        } else {
          // Risk: the process might have just completed, but then it is set to error
          await processService.setRunningProcessToError(process, new Error('Stalled process.'));
        }
      }
    }
  } catch (err) {
    logger.error('Error when trying to run stalled running processes. %s', err.message, err.stack || err);
    // anyway it will be tried later on
  } finally {
    if (lock) {
      lock.release();
    }
  }
  // some processes might still be able to execute;
  service._scheduleToCheckForNewProcessResquests();
}

/**
 * Check if there are new processes and the queue
 * If the queue is currently being process, wait for processing again as new requests
 * might have been submitted while system was locking the queue.
 *
 * This prevents from checking multiple times the queue after the queue was read but only once.
 * It is not necessary and would be put extra load on redis to only rely on its lock.
 *
 */
function _scheduleToCheckForNewProcessResquests() {
  if (!waitForProcessingQueue) {
    runNextProcesses();
  } else {
    waitForProcessingQueue.then(() => {
      if (!waitForProcessingQueue) {
        runNextProcesses();
      }
    });
  }
}

async function runNextProcesses() {
  let done;
  waitForProcessingQueue = new Promise((resolve) => {
    done = resolve;
  });

  if (isServerShuttingDown()) {
    return;
  }
  const nextProcessesToRun = await service._selectNextProcessesToRun(serverStatusService.getServerId());
  done();
  waitForProcessingQueue = null;

  const runningProcesses = _.map(nextProcessesToRun, async (process) => {
    try {
      activeProcesses[process.id] = process;
      await service._executeProcess(process, serverStatusService.getServerId());
      delete activeProcesses[process.id];
    } catch (err) {
      delete activeProcesses[process.id];
    }
  });

  if (runningProcesses.length) {
    try {
      await Promise.race(runningProcesses);
    } catch (err) {
      // process seems to have crashed.
    }
    // let's check if there is more processes in the queue that
    // were added but not processed after their request notification by any server due to capacity restriction
    service._scheduleToCheckForNewProcessResquests();
  }
}


function isServerShuttingDown() {
  return zerv.isServerShutDownInProgress();
}


async function _selectNextProcessesToRun(serverId) {
  if (capacityProfile.isServerAtFullCapacity()) {
    logger.debug('Server is currently running at full capacity %b and will not accept more processes for now', capacityProfile);
    return;
  }
  // Why a Lock?
  // When the current process is updating the status  to in IN_PROGRESS
  // Another  process/server might be requesting for permission to set the status at the same time for the same entity integration.
  // The integration request will not be able to check the process status until the first process has completed updating the status.
  // the other integration will end up not starting avoiding multiple processes running on the same data.

  // When the transaction is completed, the lock is released.
  // this makes sure no other server will try to run process at the same time
  // other servers are STILL available to run a different sync on other entity/direction to distribute load.
  const processesToExecute = [];
  let lock;
  try {
    lock = await queueLockService.getPendingQueueLock();

    // the queue returns all records that are not current locked by any other transaction
    const queue = await processService.findAllPendingProcessesByTypes(_.keys(supportedProcessTypes));
    capacityProfile.orderQueue(queue);

    logger.debug('Pending Queue: ', JSON.stringify(_.map(queue, p => _.pick(p, ['type', 'name', 'progressDescription'])), null, 3));

    for (const process of queue) {
      if (isServerShuttingDown() || capacityProfile.isServerAtFullCapacity()) {
        break;
      }
      if (capacityProfile.canServerExecuteProcessNow(process)) {
        activeProcesses[process.id] = process;
        await processService.setPendingProcessToInProgress(process, serverId);
        processesToExecute.push(process);
      }
    }
  } catch (err) {
    logger.error('Error when selecting next process to run. %s', err.message, err.stack || err);
    // some processes might be able to execute;
  } finally {
    if (lock) {
      lock.release();
    }
  }
  return processesToExecute;
}


/**
 *
 * Check if the process is not stalled. It means that it does not remain in progress forever.
 * This could happen if the process crashes with incorrect error handling (should not happen) or that the server that handle the process has crashed or rebooted.
 *
 * Note:
 * Currently a grace period is used to define is the process is valid.
 * but a true solution would be to detect if a server is no longer alive - Which would mean the process will never complete and should be put back to pending.
 *
 *
 * @param {String} serverId
 * @param {TenantProcess} process
 *
 * @returns {boolean} true if the process is stalled
 */
function checkIfProcessIsStalled(process) {
  if (process.status !== processService.PROCESS_STATUSES.IN_PROGRESS) {
    return false;
  }
  // Only pulling data from an external system can take substantial time. (ex pulling all projects or timesheets in idb)
  // the following grace period gives enough time to complete the fetch, without misleading the integration to believe the process has crashed and restart a same process (same entity, same direction) while the previous one has not completed.
  const gracePeriodInMinutes = supportedProcessTypes[process.type].gracePeriodInMins || (2 * 60); // hours
  const lastUpdateDuration = moment.duration(moment().diff(process.lastModifiedDate));
  logger.trace('%s: This process has been running on %b for %s since last update.', process.display, process.serverId, lastUpdateDuration.humanize());

  if (lastUpdateDuration.asMinutes() > gracePeriodInMinutes) {
    logger.warn('The current process %b on %b seems stalled and did not update its progress for %s. gracePeriodInMinutes is %b. Server/Process might have crashed or been interrupted.', process.display, process.serverId, lastUpdateDuration.humanize(), moment.duration(gracePeriodInMinutes, 'minutes').humanize());
    return true;
  }

  const serverOwner = serverStatusService.findByServerId(process.serverId);

  if (!serverOwner) {
    const noServerResponseTime = moment.duration(moment().diff(thisServerStart)).asSeconds();
    if (noServerResponseTime >= serverStatusService.getServerTimeoutInSecs()) {
      // usually servers start quickly and notify their presence but the server that started this process has still not notified its presence
      // since this server started
      logger.warn('The current process %b on %b is stalled. No response for %ssecs from the server %b which started the process. It must be down.', process.display, process.serverId, noServerResponseTime);
      return true;
    }
  } else if (!moment(serverOwner.start).isBefore(process.start)) {
    logger.warn('The current process %b on %b is stalled. The server %b which started the process was rebooted.', process.display, process.serverId);
    return true;
  } else if (!serverOwner.isAlive()) {
    logger.warn('The current process %b on %b is stalled. The server %b which started the process is offline.', process.display, process.serverId);
    return true;
  }

  return false;
}


/**
 * Execute the process implementation and update its status thru the execution
 * @param {TenantProcess} process
 * @param {String} byServer
 */
async function _executeProcess(process, byServer) {
  const supportedProcessType = supportedProcessTypes[process.type];
  const executeFn = supportedProcessType.execute;
  process.status = processService.PROCESS_STATUSES.IN_PROGRESS;
  process.progressDescription = 'Started';
  process.serverId = byServer;
  process.start = new Date();
  process.end = null;

  let nextRun = {scheduledData: process.scheduledDate, intervalInSecs: process.intervalInSecs};
  ;

  const processHandle = await createProcessHandle(process);
  try {
    await processHandle.setProgressDescription(`Started by server ${byServer}`);

    const result = await executeFn(process.tenantId, processHandle, process.params);
    process.status = processService.PROCESS_STATUSES.COMPLETE;
    process.progressDescription = result.description;
    // this amount of data will be notified to the entire cluster. Careful!
    // Later on, pass a dataId that would help locate the result in a temporary location as SalesForce does.
    process.data = result.data;
    process.end = new Date();
    processHandle.done();
    logger.debug('%s: Completed successfully after %s seconds. Result: %s', process.display, process.duration, process.progressDescription);

    const lock = await queueLockService.getScheduleQueueLock(process.id);
    try {
      const existingProcess = await processService.findActiveProcessById(process.id);
      if (!existingProcess) {
        logger.error('process missing');
        return;
      }
      if (existingProcess.status !== processService.PROCESS_STATUSES.IN_PROGRESS) {
        logger.error('process is incorrect state: %s',process.status);
        return;
      }
      nextRun = {scheduledData: existingProcess.scheduledDate, intervalInSecs: existingProcess.intervalInSecs};
      await processService.updateOne(process.tenantId, process);
    } finally {
      lock.release();
    }  

  } catch (err) {
    processHandle.fail(err);
    logger.error('%s: Failure after %s seconds - %s', process.display, process.duration, err.description || err.message, err.stack || err);

    const lock = await queueLockService.getScheduleQueueLock(process.id);
    try {
      const existingProcess = await processService.findActiveProcessById(process.id);
      nextRun = {scheduledData: existingProcess.scheduledDate, intervalInSecs: existingProcess.intervalInSecs};
      await processService.setRunningProcessToError(process, err);
    } finally {
      lock.release();
    } 
  }

  // currently it is restarting even on fatal error
  if (nextRun.intervalInSecs) {
    // submit new process
    const scheduledDate = moment().add(nextRun.intervalInSecs, 'seconds').toDate();

    process = await processMonitorClient.submitProcess(
      process.tenantId,
      process.type,
      process.name,
      process.params,
      {
        single: process.single,
        scheduledDate,
        intervalInSecs: nextRun.intervalInSecs
      }
  );
    //process = restartProcess(process);
  }

  return process;
}

async function restartProcess(process) {
  // check if rescheduled or interval has changed
  const lock = await queueLockService.getScheduleQueueLock(process.id);
  try { 
    const alreadyScheduledProcess = await processService.findScheduledProcessById(process.id);
    if (alreadyScheduledProcess) {
      return alreadyScheduledProcess;
    }
  } finally {
    lock.release();
  }

  // since process does not exist in the queue, let's recreate one
  const scheduledDate = moment().add(process.intervalInSecs, 'seconds').toDate();

  process = await processMonitorClient.submitProcess(
      process.tenantId,
      process.type,
      process.name,
      process.params,
      {
        single: process.single,
        scheduledDate,
        intervalInSecs: process.intervalInSecs
      }
  );
}

async function createProcessHandle(process) {
  // activity is an object informing that the server is currently running
  // that must be awaited if the server is shutting down
  // it is also an handle on the process to update its progress
  const h = await zerv.registerNewActivity(process.type, {tenantId: process.tenantId}, {origin: 'zerv distrib'});

  h.setProgressDescription = (text) => {
    process.progressDescription = text;
    logger.info('%s: %s', process.display, text);
    return processService.updateOne(process.tenantId, process);
  };

  return h;
}
