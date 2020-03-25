const _ = require('lodash');

const zlog = require('zlog4js');
const moment = require('moment');
const queueProcessService = require('./queue-process.service');

let zerv;

const logger = zlog.getLogger('zerv/process-monitor/client');

module.exports = {
    setZervDependency,
    submitProcess,
    waitForCompletion,
};

/**
 * this function is temporary
 * it provides that an implementation to deal with reading processes in a queue.
 *
 * Note:
 * A default implementation should be implemented based on Redis
 * optional Listeners (onProcessCreation, onProcessCompletion, etc) should be implemented
 */
function setZervDependency(zervInstance) {
    zerv = zervInstance;
    listenToQueueProcessData();
}

/**
 * if a process monitor client call is waiting for a process to complete
 * the process will be in the waiting queue.
 * When the process completion is notified, the promise is the queue will be resolved and the call will be answered.
 */
const waitQueue = {};


function listenToQueueProcessData() {
  /**
     * When tenant process data is notified, All listening servers will receive the notification
     * and will be able to resolve the promises in the wait queue.
     */
    zerv.onChanges('TENANT_PROCESS_DATA', (tenantId, process, notificationType) => {
        const awaitedProcessHandler = waitQueue[process.id];
        if (awaitedProcessHandler && notificationType==='UPDATE') {
            if (process.status === 'complete') {
                return awaitedProcessHandler.resolve(process);
            } else if (process.status === 'error') {
                return awaitedProcessHandler.reject(process);
            }
        }
    });
}

/**
 * Submit a new process into the queue for one server of the cluster to process.
 * If a similar process already exists, it is returned.
 *
 * Note
 * ----
 * a browser submits via this client a new process and waits for its completion
 * if it is refreshed
 * the browser might try to submit the same process
 * but this function will return the same process instead of spawning a new one, even though the browser might have connected a different server via its socket.
 * finally the browser will get the response way faster without any load to the server.
 *
 * @param {String} tenantId
 * @param {String} type
 * @param {String} name is a uniq name/hashkey for this process type
 * @param {Object} params that will be passed to the process monitor server when it executes the implementation of this process.
 * @param {Object} options
 *    @property {Boolean} single when true, only one process with this type and name will run in the cluster. Default true.
 *
 * @returns {QueueProcess} the process that was started or the existing process
 */
async function submitProcess(tenantId, type, name, params, options = {}) {
    if (!zerv.getRedisClient()) {
        throw new Error('REDIS_DISABLED');
    }
  // by default only one process with this type and name can run in the cluster at a time
    const single = _.get(options, 'single', true);

    const existingActiveProcess = await queueProcessService.findOneByTenantIdAndTypeAndNameAndInProgressOrPending(tenantId, type, name);

    if (existingActiveProcess && single) {
    // Only one process runs enterprise wide with this exact tenantId, type and name
    // to prevent spawning the same type of process and consume too many resources.
        logger.info('%s: process already exists and has been %b on %b for %s.', existingActiveProcess.display, existingActiveProcess.status, existingActiveProcess.serverId || 'queue', moment.duration(existingActiveProcess.duration, 'seconds').humanize());
        return existingActiveProcess;
    }
    const process = new queueProcessService.QueueProcess({
        tenantId,
        type,
        name,
        params,
        status: queueProcessService.PROCESS_STATUSES.PENDING,
        single
    });
    logger.info('%s: Submitted new process into the queue.', process.display);
    await queueProcessService.createOne(tenantId, process);
    return process;
}

/**
 * Wait for the completion of a process that is run on a server of the cluster.
 * The server running the process is the one that picked up the process in the queue.
 * It will notify the client waiting for its completion when the process completed.
 *
 * @param {QueueProcess} process
 * @param {Number} timeout
 *
 * @returns {Promise} resolve with the data or reject with the error
 */
function waitForCompletion(process, timeoutInSecs = 30) {
    logger.info('%s: Waiting for completion...', process.display);
    let awaitedProcessHandler = waitQueue[process.id];
    if (awaitedProcessHandler) {
        return awaitedProcessHandler.promise;
    }
    awaitedProcessHandler = {process};
    const promise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            process.status = queueProcessService.PROCESS_STATUSES.ERROR;
            process.error = `The process did not complete execution in less than ${timeoutInSecs} seconds. check REDIS or increase timeoutInSecs taking in consideration server resources and process duration.`;
            awaitedProcessHandler.reject(process);
        }, timeoutInSecs * 1000);

    // when the server executing the process completes, it will notify the process.
    // This server is listening to process notifications
    // It will resolve and reject accordingly if the process is in its waitqueue.
        awaitedProcessHandler.resolve = (processData) => {
            logger.info('%s: Completed - %s', processData.display, processData.progressDescription);
            delete waitQueue[awaitedProcessHandler.process.id];
            clearTimeout(timeout);
            resolve(processData.data);
        };
        awaitedProcessHandler.reject = (processData) => {
            logger.info('%s: Error', processData.display, processData.error);
            delete waitQueue[awaitedProcessHandler.process.id];
            clearTimeout(timeout);
            reject(processData.error);
        };
    });
    awaitedProcessHandler.promise = promise;
    waitQueue[process.id] = awaitedProcessHandler;
    return awaitedProcessHandler.promise;
}
