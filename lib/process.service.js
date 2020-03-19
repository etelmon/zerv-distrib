const
  _ = require('lodash'),
  UUID = require('uuid'),
  zlog = require('zlog4js');
const moment = require('moment');


const logger = zlog.getLogger('zerv/distrib/processService');

const QUEUES = {
  // the pending queue are all processes that await to be executed
  // ordering this queue based on some priorities can be key to keep the system responsive on the most important processes
  PENDING_QUEUE: 'zerv-pending-queue',
  // In progress queue contains the process that are currently in progress, though some might be stalled
  IN_PROGRESS_QUEUE: 'zerv-in-progress-queue',
  // Scheduled queue contains all processes that have a sheduled data
  SCHEDULED_QUEUE: 'zerv-scheduled-queue',
  // Done queue contains all processes that have recently completed
  // This should give enough information to figure out the recent load to adjust load balancing
  DONE_QUEUE: 'zerv-inactive-queue',
  // the detail process objects that are not done 
  ACTIVE_QUEUE: 'zerv-active-queue',

}

const PROCESS_STATUSES = {
  SCHEDULED: 'scheduled',
  IN_PROGRESS: 'in progress',
  COMPLETE: 'complete',
  ERROR: 'error',
  PENDING: 'pending',
  CANCELED: 'canceled',
};


class TenantProcess {
  constructor(obj) {

    this.single = obj.scheduledDate || obj.intervalInSecs ? true : (obj.single || false);

    this.id = this.single ? formatSingleProcessId(obj.tenantId, obj.type, obj.name) : (obj.id || UUID.v4());
    this.createdDate = getDate(obj.createdDate);
    this.lastModifiedDate = getDate(obj.lastModifiedDate);

    this.type = obj.type || null;
    this.name = obj.name || null;
    this.tenantId = obj.tenantId || null;
    this.params = obj.params || {};

    this.revision = obj.revision || 0;

    this.start = getDate(obj.start);
    this.end = getDate(obj.end);
    this.status = obj.status || PROCESS_STATUSES.PENDING;
    this.scheduledDate = getDate(obj.scheduledDate);
    this.intervalInSecs = obj.intervalInSecs || 0;
    this.progressDescription = obj.progressDescription || null;
    this.serverId = obj.serverId || null;

    this.data = obj.data || null;
    this.error = obj.error || null;
  }
  get display() {
    return `Process ${this.id} [${this.type}/${this.name}] for tenant ${this.tenantId}`;
  }

  get duration() {
    let duration;
    if (this.status !== PROCESS_STATUSES.IN_PROGRESS) {
      duration = moment.duration(moment(this.end).diff(this.start));
    } else {
      duration = moment.duration(moment().diff(this.start));
    }
    return duration.asSeconds();
  }
}

function getDate(dateString) {
  if (_.isDate(dateString)) {
    return dateString;
  }
  return _.isEmpty(dateString) ? null : new Date(dateString);
}

const service = {
  setZervDependency,
  createOne,
  updateOne,
  updateActiveProcess,
  setScheduledProcessToPending,
  setRunningProcessBackToPending,
  setPendingProcessToInProgress,
  setRunningProcessToError,

  findAll,
  findAllInProgressProcessesByTypes,
  findAllPendingProcessesByTypes,
  findAllScheduledProcessesByTypes,

  findByTenantIdAndTypeAndNameAndInPendingOrScheduled,
  findOneByTenantIdAndTypeAndNameAndInProgressOrPending,
  findScheduledProcessById,

  cancelProcess,
  formatSingleProcessId,
  findActiveProcessById,

  TenantProcess,
  PROCESS_STATUSES,
  QUEUES,
};

module.exports = service;

let zerv;

function formatSingleProcessId(tenantId, type, name) {
  return tenantId + type + name ;
}
function setZervDependency(zervInstance) {
  zerv = zervInstance;
}

async function findActiveProcessById(processId) {
  let ps = await zerv.getRedisClient().hget(QUEUES.IN_PROGRESS_QUEUE,processId);

  const ps2 = await zerv.getRedisClient().hget(QUEUES.PENDING_QUEUE, processId);
  
  const ps3 = await zerv.getRedisClient().hget(QUEUES.SCHEDULED_QUEUE, processId);

  ps = ps || ps2 || ps3;
  if (ps) {
    return new TenantProcess(JSON.parse(ps));
  }
  return null;
}

async function findAllInProgressProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(QUEUES.IN_PROGRESS_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAllPendingProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(QUEUES.PENDING_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findScheduledProcessById(processId) {
  const p = await zerv.getRedisClient().hget(QUEUES.SCHEDULED_QUEUE, processId); 
  if (p) {
    return new TenantProcess(JSON.parse(p));
  }
  return null;
}

async function findAllScheduledProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(QUEUES.SCHEDULED_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAll() {
  const max = process.env.ZERV_QUEUE_MAX || 1000;
  const inProgressPs = await zerv.getRedisClient().hvals(QUEUES.IN_PROGRESS_QUEUE);
  const pendingPs = await zerv.getRedisClient().hvals(QUEUES.PENDING_QUEUE);
  const scheduledPs = await zerv.getRedisClient().hvals(QUEUES.SCHEDULED_QUEUE);
  let inactiveCount = await zerv.getRedisClient().llen(QUEUES.DONE_QUEUE);
  const toRemove = [];
  while (max<inactiveCount--) {
    toRemove.push(['lpop', QUEUES.DONE_QUEUE]);
  }
  await zerv.getRedisClient().pipeline(toRemove).exec();
  const inactives = await zerv.getRedisClient().lrange(QUEUES.DONE_QUEUE, 0, -1);
  const data = _.concat(inProgressPs, scheduledPs, pendingPs, inactives);
  return _.map(data, p => new TenantProcess(JSON.parse(p)));
}

async function findByTenantIdAndTypeAndNameAndInPendingOrScheduled(tenantId, type, name) {
  const data = _.concat(
      await service.findAllPendingProcessesByTypes([type]),
      await service.findAllScheduledProcessesByTypes([type])
  );
  const result = _.filter(data, (process) => process.tenantId === tenantId && process.name === name);
  return result;
}

async function findOneByTenantIdAndTypeAndNameAndInProgressOrPending(tenantId, type, name) {
  const data = _.concat(
      await service.findAllInProgressProcessesByTypes([type]),
      await service.findAllPendingProcessesByTypes([type]),
  );
  const result = _.find(data, (process) => process.tenantId === tenantId && process.name === name);
  return result;
}

async function createOne(tenantId, process) {
  process.createdDate = new Date();
  process.lastModifiedDate = new Date();
  if (process.status === PROCESS_STATUSES.SCHEDULED) {
    await zerv.getRedisClient().hset(QUEUES.SCHEDULED_QUEUE, process.id, JSON.stringify(process));
  } else {
    await zerv.getRedisClient().hset(QUEUES.PENDING_QUEUE, process.id, JSON.stringify(process));
  }
  logger.info('Added process %b/%b to processing queue %b', process.type, process.name, process.status);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyCreation(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});

  return process;
}

async function updateOne(tenantId, process) {
  process.revision++;
  process.lastModifiedDate = new Date();
  if (process.status === PROCESS_STATUSES.SCHEDULED) {
    await zerv.getRedisClient().hset(QUEUES.SCHEDULED_QUEUE, process.id, JSON.stringify(process));
  } else if (process.status === PROCESS_STATUSES.IN_PROGRESS) {
    await zerv.getRedisClient().hset(QUEUES.IN_PROGRESS_QUEUE, process.id, JSON.stringify(process));
  } else {
    await zerv.getRedisClient().hdel(QUEUES.IN_PROGRESS_QUEUE, process.id);
    await zerv.getRedisClient().rpush(QUEUES.DONE_QUEUE, JSON.stringify(process));
  }
  logger.info('Updated process %b/%b in processing queue: %b %s', process.type, process.name, process.status, process.progressDescription);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyUpdate(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function updateActiveProcess(tenantId, process) {

  
}

async function setScheduledProcessToPending(process) {
  await zerv.getRedisClient().hdel(QUEUES.SCHEDULED_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.PENDING;
  await zerv.getRedisClient().hset(QUEUES.PENDING_QUEUE, process.id, JSON.stringify(process));
  // notify so that the queue is scheduled to process it.
  // Yes, notifying the full object in redis could be avoided with further optimization
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setRunningProcessBackToPending(process) {
  await zerv.getRedisClient().hdel(QUEUES.IN_PROGRESS_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.PENDING;
  await zerv.getRedisClient().hset(QUEUES.PENDING_QUEUE, process.id, JSON.stringify(process));
  // notify so that the queue is scheduled to process it.
  // Yes, notifying the full object in redis could be avoided with further optimization
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setRunningProcessToError(process, err) {
  await zerv.getRedisClient().hdel(QUEUES.IN_PROGRESS_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.ERROR;
  process.error = _.isError(err) ? {message: err.message, description: err.description} : err;
  process.end = new Date();
  await zerv.getRedisClient().rpush(QUEUES.DONE_QUEUE, JSON.stringify(process));
  // notify so that the queue is scheduled to process it.
  // Yes, notifying the full object in redis could be avoided with further optimization
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setPendingProcessToInProgress(process, serverId) {
  await zerv.getRedisClient().hdel(QUEUES.PENDING_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.IN_PROGRESS;
  process.start = new Date();
  process.end = null;
  process.serverId = serverId;
  process.progressDescription = `Server ${serverId} will execute this process`;
  await zerv.getRedisClient().hset(QUEUES.IN_PROGRESS_QUEUE, process.id, JSON.stringify(process));
  // Yes, notifying the full object in redis could be avoided with further optimization
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function cancelProcess(process, reason) {
 // await zerv.getRedisClient().hdel(QUEUES.IN_PROGRESS_QUEUE, process.id);
  await zerv.getRedisClient().hdel(QUEUES.PENDING_QUEUE, process.id);
  await zerv.getRedisClient().hdel(QUEUES.SCHEDULED_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.CANCELED;
  process.progressDescription = `Canceled on ${moment(process.lastModifiedDate).format('MMM Do, hh:mm:ss A')}`+ (reason ? ` - ${reason}` : '');
  await zerv.getRedisClient().rpush(QUEUES.DONE_QUEUE, JSON.stringify(process));
  // Yes, notifying the full object in redis could be avoided with further optimization
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}
