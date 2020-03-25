
const _ = require('lodash');
const moment = require('moment');
const UUID = require('uuid');


let serverStatuses = {};
const thisServerId = UUID.v4();
let serverTimeoutInSecs;
let zerv, shuttingDown, zervDefaultStopLocalServerFunction;


class ServerStatus {
    constructor(obj) {
        this.id = obj.id;
        this.type = obj.type;
        this.alive = getDate(obj.alive);
        this.start = getDate(obj.start);
        this.revision = obj.revision;
        this.activeProcesses = obj.activeProcesses || [];
        this.timeout = obj.timeout || 30;
        this.state = obj.state || 'up and running';
        this.userSessions = obj.userSessions || [];
        this.appVersion = obj.appVersion || '?';
    }

    isAlive() {
        return moment.duration(moment().diff(this.alive)).asSeconds() < serverTimeoutInSecs;
    }

    getStateDisplay() {
        if (this.isAlive()) {
            return this.state;
        } else {
            return 'offline';
        }
    }

    getShortName() {
        return this.type + '/' + this.id.slice(-6);
    }
}

const service = {
    setZervDependency,
    monitorServerStatus,
    listenToServerStatusData,
    notifyServerStatusPeriodically,
    createOne,
    updateOne,
    findAll,
    findByServerId,
    getServerId,

    stopLocalServer,
    shutdownAllServers,

    // @ts-ignore
    ServerStatus
};

module.exports = service;

function setZervDependency(zervInstance) {
    zerv = zervInstance;
    zervDefaultStopLocalServerFunction = zerv.stopLocalServer;
}

function getDate(dateString) {
    if (_.isDate(dateString)) {
        return dateString;
    }
    return _.isEmpty(dateString) ? null : new Date(dateString);
}


/**
 * This function monitors all server statuses
 * - it notifies periodically to the cluster that this local server is alive 
 * - it listens to server status updated from the cluster (shutdown, other server activities for reporting purposes)
 *
 * @param {String} serverName  the name or type of server
 * @param {String} appVersion which application version this server is running (used for logging only currently)
 * @param {Object} options
 *      @property {Number} serverStayAliveInSecs how often the server notifies that it is alive
 *      @property {Number} serverStayAliveTimeoutInSecs how long before server is considered offline
 *     
 */
function monitorServerStatus(serverName, appVersion, options = {}) {
    // listen to all server status to detect when they are no longer alive
    // this will help to manage stalled process
    service.listenToServerStatusData();
    const thisServerStatus = service.notifyServerStatusPeriodically(
        serverName, 
        {
            appVersion,
            serverStayAliveInSecs: options.serverStayAliveInSecs || process.env.ZERV_STAY_ALIVE,
            serverStayAliveTimeoutInSecs: options.serverStayAliveTimeoutInSecs || process.env.ZERV_STAY_ALIVE_TIMEOUT
        }
    );
    return thisServerStatus;
}

function listenToServerStatusData() {
    // Augmente Zerv capabilities to shutdown servers in a cluster
    // comments are below in source file
    zerv.stopLocalServer = service.stopLocalServer;
    zerv.shutdown = service.shutdownAllServers;

    serverStatuses = {};
    // all servers are aware of the status of all other servers
    // this could be useful to optimize server capacity later on.
    zerv.onChanges('SERVER_STATUS_DATA', (tenantId, serverStatus, notificationType) => {
        serverStatuses[serverStatus.id] = new ServerStatus(serverStatus);
    });

    zerv.onChanges('SERVER_SHUTDOWN', (tenantId, server, notificationType) => {
        if (server.serverId === zerv.getServerId()) {
            const serverStatus = findByServerId(server.serverId);
            if (serverStatus.state !== 'request shutdown') {
                serverStatus.state = 'request shutdown';
                updateOne(serverStatus);
                // stop and shutdown
                zerv.stopLocalServer(1, 1);
            }
        }
    });

    // delete dead server after a while. what is the point of seeing the server in the active list offline.
    // by default let's keep them an hour
    const clearDeadServerInSecs = Number(process.env.ZERV_DEAD_SERVER_CLEAR_IN_SECS || (60 * 60));
    setInterval(
        () => {
            const list = service.findAll();
            _.forEach(list, (serverStatus) => {
                if (!serverStatus.isAlive() && moment.duration(moment().diff(serverStatus.alive)).asSeconds() > clearDeadServerInSecs) {
                    delete serverStatuses[serverStatus.id];
                }
            });
        },
        clearDeadServerInSecs * 1000 / 2
    );
}

/**
 * notify this server status periodically
 * so that other servers will know if a process is no longer handled properly
 */
function notifyServerStatusPeriodically(serverName, options = {}) {

    const serverStayAliveInSecs = options.serverStayAliveInSecs || 30;

    serverTimeoutInSecs = Math.max(
        // a server is considered down if it misses 4 times to signal its presence to others
        // a busy server might not informed its presence often.
        serverStayAliveInSecs * 4,
        // or if timeout is provided (and big enough) let's use it instead
        // which would mean servers are way too busy
        // and there is load balancer stragegy issue.
        options.serverStayAliveTimeoutInSecs || 0
    );

    const thisServerStatus = service.createOne(serverName, options.appVersion || 'No Version', serverStayAliveInSecs);
    setInterval(() => {
        const serverStatus = service.findByServerId(thisServerId);
        // Though notifying is supposed to be infrequent, still need to reduce excessive amount of data...
        serverStatus.activeProcesses = zerv.getActivitiesInProcess();
        serverStatus.userSessions = zerv.getLocalUserSessions();
        service.updateOne(serverStatus);
    }, serverStayAliveInSecs * 1000);

    return thisServerStatus;
}

/**
 * create this server status and notify a server status object to the whole zerv cluster
 * It is useful for notifying the current load of the server for monitoring purposes.
 *
 * @param {String} type
 * @param {Number} timeout  which is the number of seconds before a server is considered not responsive.
 */
function createOne(type, appVersion, timeout) {
    const serverStatus = new ServerStatus({
        id: thisServerId,
        type,
        alive: new Date(),
        revision: Date.now(), // important on server restart
        start: new Date(),
        appVersion,
        timeout
    });
    serverStatuses[serverStatus.id] = serverStatus;
    zerv.notifyCreation('cluster', 'SERVER_STATUS_DATA', serverStatus, { allServers: true });

    return serverStatus;
}

function updateOne(serverStatus) {
    serverStatus.revision++;
    serverStatus.alive = new Date();
    zerv.notifyUpdate('cluster', 'SERVER_STATUS_DATA', serverStatus, { allServers: true });
}

function findAll() {
    return _.values(serverStatuses);
}

function findByServerId(serverId) {
    return serverStatuses[serverId];
}

function getServerId() {
    return thisServerId;
}


// ---------------------------------------------------------------------------------
// Quick implementation of experimental functionalities not unit tested but working
// however, currently missing ability to shutdown a specific server in the cluster.
// ---------------------------------------------------------------------------------
async function stopLocalServer(delay, exitDelay = 0) {
    if (!zerv) {
        throw new Error('listenToServerStatusData was not executed');
    }
    let serverStatus = service.findByServerId(service.getServerId());
    serverStatus.state = 'shutdown in progress';
    service.updateOne(serverStatus, []);
    await zervDefaultStopLocalServerFunction(delay);
    serverStatus = service.findByServerId(service.getServerId());
    serverStatus.state = 'shutdown';
    service.updateOne(serverStatus, []);
    if (exitDelay) {
        setTimeout(() => {
            console.info('shutdown local server');
            process.exit();
        }, exitDelay * 1000);
    }
};

/**
   * Shutdown and exit the server after all activities currently in progress completed on all zerv servers
   * @param {Number} delayBeforeShuttingdown
   * @param {Number} postDelay is the delay after stopping all activies before exiting
   */
async function shutdownAllServers(delayBeforeShuttingdown = 5, exitDelay = 5) {
    if (!zerv) {
        throw new Error('listenToServerStatusData was not executed');
    }
    if (shuttingDown) {
        return;
    }
    shuttingDown = setTimeout(() => {
        const serverStatuses = service.findAll();
        _.forEach(serverStatuses, (serverStatus) => {
            if (serverStatus.id !== zerv.getServerId()) {
                zerv.notifyCreation('cluster', 'SERVER_SHUTDOWN', { serverId: serverStatus.id }, { allServers: true });
            }
        });
    }, delayBeforeShuttingdown);
    zerv.stopLocalServer(delayBeforeShuttingdown, false);
    let h;
    await new Promise((resolve) => {
        // check that all servers are down on timely basis
        h = setInterval(() => {
            const newServerStatuses = service.findAll();
            if (_.every(newServerStatuses, (serverStatus) => serverStatus.state === 'shutdown' || !serverStatus.isAlive())) {
                console.log(JSON.stringify(newServerStatuses, null, 2));
                resolve();
            }
        }, 1000);
    });
    clearInterval(h);
    setTimeout(() => {
        console.info('shutdown of the entire infrastructure completed');
        process.exit();
    }, exitDelay * 1000);
};
