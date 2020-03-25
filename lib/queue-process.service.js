const
    _ = require('lodash'),
    UUID = require('uuid'),
    zlog = require('zlog4js');
const moment = require('moment');

const ACTIVE_QUEUE = 'zerv-active-queue';
const INACTIVE_QUEUE = 'zerv-inactive-queue';
const logger = zlog.getLogger('zerv/distrib/queueProcessService');

const PROCESS_STATUSES = {
    IN_PROGRESS: 'in progress',
    COMPLETE: 'complete',
    ERROR: 'error',
    PENDING: 'pending'
};


class QueueProcess {
    constructor(obj) {
        this.id = obj.id || UUID.v4();

        // this.single = obj.scheduledDate || obj.intervalInSecs ? true : (obj.single || false);

        // this.id = this.single ? formatSingleProcessId(obj.tenantId, obj.type, obj.name) : (obj.id || UUID.v4());

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


module.exports = {
    setZervDependency,
    createOne,
    updateOne,
    findOneByTenantIdAndTypeAndNameAndInProgressOrPending,
    findAllPendingAndInProgressProcesses,
    findAll,

    // findActiveProcess,
    // formatSingleProcessId,

    QueueProcess,
    PROCESS_STATUSES,
};

let zerv;

function setZervDependency(zervInstance) {
    zerv = zervInstance;
}

// function formatSingleProcessId(tenantId, type, name) {
//   return tenantId + type + name ;
// }

// async function findActiveProcess(id) {
//   const p = await zerv.getRedisClient().hget(ACTIVE_QUEUE, id);
//   if (p) {
//     return new QueueProcess(JSON.parse(p));
//   }
//   return null;
// }

async function findAllPendingAndInProgressProcesses(processTypes) {
    const actives = await zerv.getRedisClient().hvals(ACTIVE_QUEUE);
    const data = _.map(actives, p => new QueueProcess(JSON.parse(p)));
    const result = _.filter(data, (process) => _.some(processTypes, type => process.type === type));
    return result;
}

async function findAll() {
    const max = process.env.ZERV_QUEUE_MAX || 1000;
    const actives = await zerv.getRedisClient().hvals(ACTIVE_QUEUE);
    let inactiveCount = await zerv.getRedisClient().llen(INACTIVE_QUEUE);
    const toRemove = [];
    while (max < inactiveCount--) {
        toRemove.push(['lpop', INACTIVE_QUEUE]);
    }
    await zerv.getRedisClient().pipeline(toRemove).exec();
    const inactives = await zerv.getRedisClient().lrange(INACTIVE_QUEUE, 0, -1);
    const data = _.concat(actives, inactives);
    return _.map(data, p => new QueueProcess(JSON.parse(p)));
}

async function findOneByTenantIdAndTypeAndNameAndInProgressOrPending(tenantId, type, name) {
    name = name || null;
    const actives = await findAllPendingAndInProgressProcesses([type]);
    const result = _.find(actives, (process) => process.name === name);
    return result;
}

async function createOne(tenantId, process) {
    process.lastModifiedDate = new Date();
    process.createdDate = new Date();

    await zerv.getRedisClient().hset(ACTIVE_QUEUE, process.id, JSON.stringify(process));

    logger.info('Added process %b/%b to processing queue', process.type, process.name);
    // let's send the notifications to all servers so that they might execute/handle the process
    zerv.notifyCreation(tenantId, 'TENANT_PROCESS_DATA', process, { allServers: true });

    return process;
}

async function updateOne(tenantId, process) {
    process.revision++;
    process.lastModifiedDate = new Date();

    if (process.status === PROCESS_STATUSES.PENDING || process.status === PROCESS_STATUSES.IN_PROGRESS) {
        await zerv.getRedisClient().hset(ACTIVE_QUEUE, process.id, JSON.stringify(process));
    } else {
        await zerv.getRedisClient().hdel(ACTIVE_QUEUE, process.id);
        await zerv.getRedisClient().rpush(INACTIVE_QUEUE, JSON.stringify(process));
    }

    logger.info('Updated process %b/%b in processing queue: %b %s', process.type, process.name, process.status, process.progressDescription);
    // let's send the notifications to all servers so that they might execute/handle the process
    zerv.notifyUpdate(tenantId, 'TENANT_PROCESS_DATA', process, { allServers: true });
    return process;
}

function getDate(dateString) {
    if (_.isDate(dateString)) {
        return dateString;
    }
    return _.isEmpty(dateString) ? null : new Date(dateString);
}
