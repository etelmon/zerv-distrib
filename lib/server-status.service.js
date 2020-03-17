
const _ = require('lodash');
const moment = require('moment');
const UUID = require('uuid');


let serverStatuses = {};
const thisServerId = UUID.v4();
let zerv;


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
    return moment.duration(moment().diff(this.alive)).asSeconds() < this.timeout;
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
  listenToServerStatusData,
  notifyServerStatusPeriodically,
  createOne,
  updateOne,
  findAll,
  findByServerId,
  getServerId,

  // @ts-ignore
  ServerStatus
};

module.exports = service;

function getDate(dateString) {
  if (_.isDate(dateString)) {
    return dateString;
  }
  return _.isEmpty(dateString) ? null : new Date(dateString);
}

function listenToServerStatusData(zervInstance) {
  zerv = zervInstance;
  serverStatuses = {};
  // all servers are aware of the status of all other servers
  // this could be useful to optimize server capacity later on.
  zerv.onChanges('SERVER_STATUS_DATA', (tenantId, serverStatus, notificationType) => {
    serverStatuses[serverStatus.id] = new ServerStatus(serverStatus);
  });

  zerv.onChanges('SERVER_SHUTDOWN', (tenantId, server, notificationType) => {
    if (server.serverId === zervInstance.getServerId()) {
      const serverStatus = findByServerId(server.serverId);
      if (serverStatus.state !== 'request shutdown') {
        serverStatus.state = 'request shutdown';
        updateOne(serverStatus);
        // stop and shutdown
        zervInstance.stopLocalServer(1, 1);
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
          if (!serverStatus.isAlive() && moment.duration(moment().diff(serverStatus.alive)).asSeconds() > clearDeadServerInSecs ) {
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
function notifyServerStatusPeriodically() {
  setInterval(() => {
    const serverStatus = service.findByServerId(thisServerId);
    // need to reduce excessive amount of data...
    serverStatus.activeProcesses= zerv.getActivitiesInProcess();
    serverStatus.userSessions = zerv.getLocalUserSessions();
    service.updateOne(serverStatus);
  }, service.findByServerId(thisServerId).timeout*1000/2);
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
  zerv.notifyCreation('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});

  return serverStatus;
}

function updateOne(serverStatus) {
  serverStatus.revision ++;
  serverStatus.alive = new Date();
  zerv.notifyUpdate('cluster', 'SERVER_STATUS_DATA', serverStatus, {allServers: true});
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
