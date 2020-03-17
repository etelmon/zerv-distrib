const zlog = require('zlog4js');

const logger = zlog.getLogger('process-monitor/queue lock');


const service = {
  getScheduleQueueLock,
  getPendingQueueLock,
  setZervDependency
};

module.exports = service;

function setZervDependency(zervInstance) {
  zerv = zervInstance;
}


const ioRedisLock = require('ioredis-lock');

function getScheduleQueueLock() {
  return getBasicLock('schedule-queue');
}

function getPendingQueueLock() {
  return getBasicLock('pending-queue');
}
/**
 * This function locks the access to the queue using redis lock mechanism
 *
 * @returns {Object} with a release function to release the lock.
 */
async function getBasicLock(lockName) {
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
    await processQueueResisLock.acquire('zerv:' + lockName + ':lock');
    logger.debug('%s Locked', lockName);
    return {
      release: () => {
        processQueueResisLock.release();
        logger.debug('%s UnLocked', lockName);
      }
    };
  } catch (err) {
    // might happen when the retries is too low
    logger.error('Redis Locking error on %s: %s', lockName, err.message);
    throw new Error('LOCK_ERROR');
  }
}
