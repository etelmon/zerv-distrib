const
  _ = require('lodash'),
  UUID = require('uuid'),
  zlog = require('zlog4js');
const moment = require('moment');

const PENDING_QUEUE = 'zerv-pending-queue';
const IN_PROGRESS_QUEUE = 'zerv-in-progress-queue';
const SCHEDULED_QUEUE = 'zerv-scheduled-queue';
const DONE_QUEUE = 'zerv-inactive-queue';
const logger = zlog.getLogger('zerv/distrib/processService');

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
    this.id = obj.id || UUID.v4();
    this.createdDate = getDate(obj.createdDate);
    this.lastModifiedDate = getDate(obj.lastModifiedDate);

    this.type = obj.type || null;
    this.name = obj.name || null;
    this.tenantId = obj.tenantId || null;
    this.params = obj.params || {};
    this.single = obj.single || false; // When true only one process with this type and name can run in the cluster at a time

    this.revision = obj.revision || 0;

    this.start = getDate(obj.start);
    this.end = getDate(obj.end);
    this.status = obj.status || PROCESS_STATUSES.PENDING;
    this.scheduledDate = getDate(obj.scheduledDate);
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
  setScheduledProcessToPending,
  setRunningProcessBackToPending,
  setPendingProcessToInProgress,

  findAll,
  findAllInProgressProcessesByTypes,
  findAllPendingProcessesByTypes,
  findAllScheduledProcessesByTypes,

  findByTenantIdAndTypeAndNameAndInPendingOrScheduled,
  findOneByTenantIdAndTypeAndNameAndInProgressOrPending,

  cancelProcess,

  TenantProcess,
  PROCESS_STATUSES,
};

module.exports = service;

let zerv;

function setZervDependency(zervInstance) {
  zerv = zervInstance;
}


async function findAllInProgressProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(IN_PROGRESS_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAllPendingProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(PENDING_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAllScheduledProcessesByTypes(processTypes) {
  const actives = await zerv.getRedisClient().hvals(SCHEDULED_QUEUE);
  const data = _.map(actives, p => new TenantProcess(JSON.parse(p)));
  const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
  return result;
}

async function findAll() {
  const max = process.env.ZERV_QUEUE_MAX || 1000;
  const inProgressPs = await zerv.getRedisClient().hvals(IN_PROGRESS_QUEUE);
  const pendingPs = await zerv.getRedisClient().hvals(PENDING_QUEUE);
  const scheduledPs = await zerv.getRedisClient().hvals(SCHEDULED_QUEUE);
  let inactiveCount = await zerv.getRedisClient().llen(DONE_QUEUE);
  const toRemove = [];
  while (max<inactiveCount--) {
    toRemove.push(['lpop', DONE_QUEUE]);
  }
  await zerv.getRedisClient().pipeline(toRemove).exec();
  const inactives = await zerv.getRedisClient().lrange(DONE_QUEUE, 0, -1);
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
    await zerv.getRedisClient().hset(SCHEDULED_QUEUE, process.id, JSON.stringify(process));
  } else {
    await zerv.getRedisClient().hset(PENDING_QUEUE, process.id, JSON.stringify(process));
  }
  logger.info('Added process %b/%b to processing queue %b', process.type, process.name, process.status);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyCreation(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});

  return process;
}

async function updateOne(tenantId, process) {
  process.revision++;
  process.lastModifiedDate = new Date();

  if (process.status === PROCESS_STATUSES.IN_PROGRESS) {
    await zerv.getRedisClient().hset(IN_PROGRESS_QUEUE, process.id, JSON.stringify(process));
  } else {
    await zerv.getRedisClient().hdel(IN_PROGRESS_QUEUE, process.id);
    await zerv.getRedisClient().rpush(DONE_QUEUE, JSON.stringify(process));
  }

  logger.info('Updated process %b/%b in processing queue: %b %s', process.type, process.name, process.status, process.progressDescription);
  // let's send the notifications to all servers so that they might execute/handle the process
  zerv.notifyUpdate(tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setScheduledProcessToPending(processId) {
  let process = await zerv.getRedisClient().hget(SCHEDULED_QUEUE, processId);
  if (!process) {
    return null;
  }
  await zerv.getRedisClient().hdel(SCHEDULED_QUEUE, processId);
  process = JSON.parse(process);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.PENDING;
  await zerv.getRedisClient().hset(PENDING_QUEUE, process.id, JSON.stringify(process));
  // notify so that the queue is scheduled to process it.
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setRunningProcessBackToPending(processId) {
  let process = await zerv.getRedisClient().hget(IN_PROGRESS_QUEUE, processId);
  if (!process) {
    return null;
  }
  await zerv.getRedisClient().hdel(IN_PROGRESS_QUEUE, processId);
  process = JSON.parse(process);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.PENDING;
  await zerv.getRedisClient().hset(PENDING_QUEUE, process.id, JSON.stringify(process));
  // notify so that the queue is scheduled to process it.
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function setPendingProcessToInProgress(process, serverId) {
  await zerv.getRedisClient().hdel(PENDING_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.IN_PROGRESS;
  process.start = new Date();
  process.end = null;
  process.serverId = serverId;
  process.progressDescription = `Server ${serverId} will execute this process`;
  await zerv.getRedisClient().hset(IN_PROGRESS_QUEUE, process.id, JSON.stringify(process));
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}

async function cancelProcess(process, reason) {
  await zerv.getRedisClient().hdel(IN_PROGRESS_QUEUE, process.id);
  await zerv.getRedisClient().hdel(PENDING_QUEUE, process.id);
  await zerv.getRedisClient().hdel(SCHEDULED_QUEUE, process.id);
  process.revision++;
  process.lastModifiedDate = new Date();
  process.status = PROCESS_STATUSES.CANCELED;
  process.progressDescription = `Canceled on ${moment(process.lastModifiedDate).format('MMM Do, hh:mm:ss A')}`+ (reason ? ` - ${reason}` : '');
  await zerv.getRedisClient().rpush(DONE_QUEUE, JSON.stringify(process));
  zerv.notifyUpdate(process.tenantId, 'TENANT_PROCESS_DATA', process, {allServers: true});
  return process;
}
