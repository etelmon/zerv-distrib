const processService = require('./process.service');
const zlog = require('zlog4js');

const logger = zlog.getLogger('process-monitor/queue lock');


const service = {
  getScheduleQueueLock,
  getPendingQueueLock,
  getInProgressQueueLock,
  setZervDependency
};

module.exports = service;

function setZervDependency(zervInstance) {
  zerv = zervInstance;
}
var Redlock = require('redlock');

const ioRedisLock = require('ioredis-lock');

function getScheduleQueueLock() {
  return getBasicLock(processService.QUEUES.SCHEDULED_QUEUE);
}

function getPendingQueueLock() {
  return getBasicLock(processService.QUEUES.PENDING_QUEUE);
}

function getInProgressQueueLock() {
  return getBasicLock(processService.QUEUES.IN_PROGRESS_QUEUE);
}

/**
 * This function locks the access to the queue using redis lock mechanism
 *
 * @returns {Object} with a release function to release the lock.
 */
async function getBasicLock2(lockName) {
  if (!zerv.getRedisClient()) {
    return { release: _.noop };
  }

  const processQueueResisLock = ioRedisLock.createLock(zerv.getRedisClient(), {
    timeout: process.env.REDIS_LOCK_TIMEOUT_IN_MS || 60000,
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

async function getBasicLock(lockName, ttl = 1000) {
  if (!zerv.getRedisClient()) {
    return { release: _.noop };
  }
  var redlock = new Redlock(
    // you should have one client for each independent redis node
    // or cluster
    [zerv.getRedisClient()],
    {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // time in ms
 
        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount:  10,
 
        // the time in ms between attempts
        retryDelay:  200, // time in ms
 
        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter:  200 // time in ms
    }
  );

  try {
    const lock = await redlock.lock('zerv:' + lockName + ':lock', ttl);
    logger.debug('%s Locked', lockName);
    return {
      release: () => {
        lock.unlock();
        logger.debug('%s UnLocked', lockName);
      }
    };
  } catch (err) {
    // might happen when the retries is too low
    logger.error('Redis Locking error on %s: %s', lockName, err.message);
    throw new Error('LOCK_ERROR');
  }
}