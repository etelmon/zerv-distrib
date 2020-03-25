
const _ = require('lodash');
const zlog = require('zlog4js');
const moment = require('moment');
const ioRedisLock = require('ioredis-lock');
const serverStatusService = require('./server-status.service');
const queueProcessService = require('./queue-process.service');
const BasicCapacityProfile = require('./basic-capacity-profile');

const logger = zlog.getLogger('process-monitor/server');

const supportedProcessTypes = {};
let zerv;
let activeProcesses = {};
let _waitForProcessingQueue =null;
let capacityProfile;
const thisServerStart = new Date();

const service = {
    setZervDependency,
    monitorQueue,
    addProcessType,
    getServerId: serverStatusService.getServerId,

    _setCapacityProfile,
    _selectNextProcessesToRun,
    _scheduleToCheckForNewProcessResquests,
    _executeProcess,
    _checkIfProcessIsNotStalled,
    _runNextProcesses,
    _getActiveProcesses,
    _listenProcessQueueForNewRequests,
    _waitForProcessingQueue,
};

module.exports = service;

function setZervDependency(zervInstance) {
    zerv = zervInstance;
    activeProcesses = {};
    _waitForProcessingQueue = null;
}

function _getActiveProcesses() {
    return _.values(activeProcesses);
}

/**
 * add the type of process handled by the zerv server and its implementation
 * When process is submitted by the queue, this zerv instance will process it if capacity allows.
 * @param {String} type
 * @param {function} executeFn
 * @param {Object} options
 *  @property {Number} gracePeriodInMins       : if the process did not respond or complete in thist time, it might have crashed. so give a chance to restart it to this server
 *  @property {Number} wasteTimeInSecs         : This will add a duration to a process execution useful to simulate/test more concurrency
 *  @property {Number} priority                : when multiple processes are in the queue, the highest priority will executed first. by default priority is 10
 */
function addProcessType(type, executeFn, options = {}) {
    let execute;
    if (_.isNumber(options.wasteTimeInSecs) && options.wasteTimeInSecs > 0) {
        // delay execution to pretend the process is slow.
        execute = function(...args) {
            return new Promise((resolve, reject) => {
                setTimeout(async () => {
                    try {
                        resolve(await executeFn(...args));
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
    supportedProcessTypes[type] = _.assign({
        execute,
        priority: 10
    }, options);
}


/**
 * This function monitors the process queue 
 * 
 * Processes submitted in the queue will be executed by this server
 * only if it handles the type of process submitted and if it is not already running full capacity.
 *
 * @param {Number} capacityProfileConfig max number of processes the server can handle
 */
function monitorQueue(capacityProfileConfig) {
    if (capacityProfileConfig === 0) {
        capacityProfile = null;
    } else {
        service._setCapacityProfile(capacityProfileConfig);
    }
    // if there is no capacity or process type supported. monitor does NOT listen to the queue
    if (zerv.getRedisClient() && capacityProfile && _.keys(supportedProcessTypes).length) {
        service._listenProcessQueueForNewRequests();
    }
}


function _setCapacityProfile(config) {
    if (_.isNil(config) || _.isNumber(config)) {
        capacityProfile = new BasicCapacityProfile(
            Number(config || process.env.ZERV_MAX_PROCESS_CAPACITY || 30)
        );
        capacityProfile.setActiveProcesses(activeProcesses);
        capacityProfile.setSupportedProcessType(supportedProcessTypes);
    } else {
        throw new Error('capacity profile config not supported. Only a the number of maximum processes is supported.');
    }
    return capacityProfile;
}

/**
 * This function locks the access to the queue using redis lock mechanism
 *
 * @returns {Object} with a release function to release the lock.
 */
async function getLock() {
    if (!zerv.getRedisClient()) {
        return { release: _.noop };
    }

    const processQueueResisLock = ioRedisLock.createLock(zerv.getRedisClient(), {
        timeout: process.env.REDIS_LOCK_TIMEOUT_IN_MS || 20000,
        // retries should be based on how many servers running and how busy is the sytem
        // since many processes will try to acquire the lock at the same time on a busy system
        retries: process.env.REDIS_LOCK_RETRY || 3000,
        delay: process.env.REDIS_LOCK_DELAY_IN_MS || 100,
    });

    try {
        await processQueueResisLock.acquire('zerv:process-queue:lock');
        logger.debug('Process Queue Locked');
        return {
            release: () => {
                processQueueResisLock.release();
                logger.debug('Process Queue unLocked');
            }
        };
    } catch (err) {
        // might happen when the retries is too low
        logger.error('Redis Locking error: %s', err.message);
        throw new Error('LOCK_ERROR');
    }
}


function _listenProcessQueueForNewRequests() {
    logger.info('Server %b is monitoring process queue for executing processes %b. max capacity: %s', serverStatusService.getServerId(), _.keys(supportedProcessTypes), capacityProfile);

    zerv.onChanges('TENANT_PROCESS_DATA', async (tenantId, process, notificationType) => {
        if (process.status === queueProcessService.PROCESS_STATUSES.PENDING) {
            logger.info('Received new process in the queue: [%s/%s]', process.type, process.name);
            service._scheduleToCheckForNewProcessResquests();
        }
    });

    setTimeout(() => {
        logger.info('Checking queue for pending processes to execute as per capacity %b...', capacityProfile);
        // when the server starts, it will try to run all tasks in the queue up to its capacity;
        service._scheduleToCheckForNewProcessResquests();

        // Safety Check: every now and then,let's recheck the queue.
        // If locking mechanism fails (redis failure for ie), there might be process requests in the queue
        // not handled by any server if there is no activity in the infrastructure.
        //
        setInterval(() => {
            if (!capacityProfile.isServerAtFullCapacity()) {
                logger.debug('Process queue health check for unhandled process requests');
                service._scheduleToCheckForNewProcessResquests();
            }
        }, (process.env.ZERV_PROCESS_QUEUE_HEALTH_CHECK_IN_SECS || 60) * 1000);
    }, 5 * 1000);
}


/**
 * Check if there are new processes and the queue
 * 
 * There is some optimization to prevent checking the queue on each process notification
 * 
 * If the queue is currently being process, wait for processing again as new requests
 * might have been submitted while system was locking the queue.
 *
 */
async function _scheduleToCheckForNewProcessResquests() {
    if (!service._waitForProcessingQueue) {
        service._runNextProcesses();
    } else {
        await service._waitForProcessingQueue.then(() => {
            // maybe the processing has already started.
            if (!service._waitForProcessingQueue) {
                service._runNextProcesses();
            }
        });
    }
}

/**
 * Find out which processed needs to executed and launch their execution
 * When one of the processes completes schedule to check the queue again for new processes.
 * 
 * @returns {Promise<Number>} which is the number of processes that were launched for execution
 */
async function _runNextProcesses() {
    let done;
    service._waitForProcessingQueue = new Promise((resolve) => {
        done = resolve;
    });

    if (isServerShuttingDown()) {
        return;
    }
    const nextProcessesToRun = await service._selectNextProcessesToRun();
    done();
    service._waitForProcessingQueue = null;

    const runningProcesses = _.map(nextProcessesToRun, async (process) => {
        try {
            activeProcesses[process.id] = process;
            await service._executeProcess(process);
            delete activeProcesses[process.id];
        } catch (err) {
            delete activeProcesses[process.id];
        }
    });

    // Wait for at least one process to complete before checking the queue again
    // so that processes that could not be handled due to server capactity limit
    // are taken in consideration
    if (runningProcesses.length) {
        try {
            Promise.race(runningProcesses);
        } catch (err) {
            // process seems to have crashed.
            // this should not happen as all errors are supposed to be cached ahead
            logger.error(err.stack || err);
        }
        // some processes have just completed,
        // let's check if there are more processes in the queue that
        // were added but have not processed right by any server after their notification 
        service._scheduleToCheckForNewProcessResquests();
    }
    return runningProcesses.length;
}

function isServerShuttingDown() {
    return zerv.isServerShutDownInProgress();
}


async function _selectNextProcessesToRun() {
    const thiServerId = serverStatusService.getServerId();
    if (capacityProfile.isServerAtFullCapacity()) {
        logger.debug('Server is currently running at full capacity %b and will not accept more processes for now', capacityProfile);
        return;
    }
    // Why a Lock?
    // When the current process is updating the status  to in IN_PROGRESS
    // Another  process/server might be requesting for permission to set the status at the same time for the same entity integration.
    // The integration request will not be able to check the process status until the first process has completed updating the status.
    // the other integration will end up not starting avoiding multiple processes running on the same data.
    const processesToExecute = [];
    let lock;
    try {
        lock = await getLock();
        const queue = await queueProcessService.findAllPendingAndInProgressProcesses(_.keys(supportedProcessTypes));
        // capacity profile defines order based on prioritization implementation and restriction.
        capacityProfile.orderQueue(queue);

        logger.debug('Current Queue: ', JSON.stringify(_.map(queue, p => _.pick(p, ['type', 'name', 'status', 'serverId', 'duration'])), null, 3));

        for (const process of queue) {
            if (processesToExecute.length > capacityProfile.getMaximumNewProcessToExecute() || isServerShuttingDown() || capacityProfile.isServerAtFullCapacity()) {
                break;
            }
            if (process.status === queueProcessService.PROCESS_STATUSES.PENDING) {
                process.status = queueProcessService.PROCESS_STATUSES.IN_PROGRESS;
                process.start = new Date();
                process.end = null;
                process.serverId = thiServerId;
                process.progressDescription = `Server ${thiServerId} will execute this process`;
                activeProcesses[process.id] = process;
                await queueProcessService.updateOne(process.tenantId, process);
                processesToExecute.push(process);
            } else {
                // the process is in progress. Is it stalled?
                {
                    try {
                        service._checkIfProcessIsNotStalled(process);
                    } catch (err) {
                        process.status = queueProcessService.PROCESS_STATUSES.ERROR;
                        process.end = new Date();
                        process.error = _.isError(err) ? { message: err.message, description: err.description } : err;
                        delete activeProcesses[process.id];
                        await queueProcessService.updateOne(process.tenantId, process);
                    }
                }
            }
        }
    } catch (err) {
        logger.error('Error when selecting next process to run. %s', err.message, err.stack || err);
        // some processes might be able to execute
    } finally {
        if (lock) {
            await lock.release();
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
 * @param {QueueProcess} process
 *
 * @throws {Error} if process is stalled
 */
function _checkIfProcessIsNotStalled(process) {
    if (process.status !== queueProcessService.PROCESS_STATUSES.IN_PROGRESS) {
        return false;
    }
    // Only pulling data from an external system can take substantial time. (ex pulling all projects or timesheets in idb)
    // the following grace period gives enough time to complete the fetch, without misleading the integration to believe the process has crashed and restart a same process (same entity, same direction) while the previous one has not completed.
    const gracePeriodInMinutes = supportedProcessTypes[process.type].gracePeriodInMins || (2 * 60); // hours
    const lastUpdateDuration = moment.duration(moment().diff(process.lastModifiedDate));
    logger.trace('%s: This process has been running on %b for %s since last update.', process.display, process.serverId, lastUpdateDuration.humanize());

    if (lastUpdateDuration.asMinutes() > gracePeriodInMinutes) {
        logger.warn('The current process %b on %b seems stalled and did not update its progress for %s. gracePeriodInMinutes is %b. Server/Process might have crashed or been interrupted.', process.display, process.serverId, lastUpdateDuration.humanize(), moment.duration(gracePeriodInMinutes, 'minutes').humanize());
        throw new Error(`Failed to complete in time. Process grace Period of ${gracePeriodInMinutes} minutes is over.`);
    }

    const serverOwner = serverStatusService.findByServerId(process.serverId);

    if (!serverOwner) {
        const timeSinceLocalServerStarted = moment.duration(moment().diff(thisServerStart)).asSeconds();
        const serverTimeout = serverStatusService.getServerTimeoutInSecs();
        if (timeSinceLocalServerStarted >= serverTimeout) {
            // this server must have been started recently.
            // the server that started this process has still not notified its presence
            // since this server started
            logger.warn('The current process %b on %b is stalled. No response for %ssecs from the server %b which started the process. It must be down.', process.display, process.serverId, timeSinceLocalServerStarted);
            throw new Error(`Server owner timeout. No response for more than ${serverTimeout} secs since this server rebooted.`);
        }
    } else if (!moment(serverOwner.start).isBefore(process.start)) {
        logger.warn('The current process %b on %b is stalled. The server %b which started the process was rebooted.', process.display, process.serverId);
        throw new Error('This server rebooted before it completed process');
    } else if (!serverOwner.isAlive()) {
        logger.warn('The current process %b on %b is stalled. The server %b which started the process is offline.', process.display, process.serverId);
        throw new Error('Server process owner is no longer alive');
    }

    return false;
}


/**
 * Execute the process implementation and update its status thru the execution
 * @param {QueueProcess} process
 * @returns {Promise<QueueProcess} which is the process updated with its completion status
 */
async function _executeProcess(process) {
    const thisServerId = serverStatusService.getServerId();
    const supportedProcessType = supportedProcessTypes[process.type];
    const executeFn = supportedProcessType.execute;
    process.status = queueProcessService.PROCESS_STATUSES.IN_PROGRESS;
    process.progressDescription = 'Started';
    process.serverId = thisServerId;
    process.start = new Date();
    process.end = null;

    // activity is an object informing that the server is currently running
    // that must be awaited if the server is shutting down
    // it is also an handle on the process to update its progress
    const processHandle = await zerv.registerNewActivity(process.type, { tenantId: process.tenantId }, { origin: 'zerv distrib' });
    processHandle.setProgressDescription = setProgressDescription;
    try {
        await setProgressDescription(`Started by server ${thisServerId}`);

        const result = await executeFn(process.tenantId, processHandle, process.params);
        process.status = queueProcessService.PROCESS_STATUSES.COMPLETE;
        process.progressDescription = result.description;
        // CAREFUL! this amount of data will be notified to redis and the entire cluster.
        // For large amount of data, pass instead some ids in data that would help locate the result.
        process.data = result.data;
        process.end = new Date();
        processHandle.done();
        logger.debug('%s: Completed successfully after %s seconds. Result: %s', process.display, process.duration, process.progressDescription);
    } catch (err) {
        process.status = queueProcessService.PROCESS_STATUSES.ERROR;
        process.error = _.isError(err) ? { message: err.message, description: err.description } : err;
        process.end = new Date();
        processHandle.fail(err);
        logger.error('%s: Failure after %s seconds - %s', process.display, process.duration, err.description || err.message, err.stack || err);
    }
    return await queueProcessService.updateOne(process.tenantId, process);


    async function setProgressDescription(text) {
        process.progressDescription = text;
        logger.info('%s: %s', process.display, text);
        await queueProcessService.updateOne(process.tenantId, process);
    }
}
