
'strict mode';

const _ = require('lodash');
const moment = require('moment');


let zervCore;
_.forIn(require.cache, function(required) {
  if (required.exports && required.exports.apiRouter) {
    zervCore = required.exports;
    return false;
  }
});
if (!zervCore) {
  zervCore = require('zerv-core');
}

let zervSync;
_.forIn(require.cache, function(required) {
  if (required.exports && required.exports.notifyCreation) {
    zervSync = required.exports;
    return false;
  }
});
if (!zervSync) {
  zervSync = require('zerv-sync');
}


let zervDistrib;
_.forIn(require.cache, function(required) {
  if (required.exports && required.exports.submitProcess) {
    zervDistrib = required.exports;
    return false;
  }
});

// prevent creating another instance of this module
if (zervDistrib) {
  module.exports = zervDistrib;
} else {
  const IoRedis = require('ioredis');

  const processMonitorClient = require('./process-monitor-client.service');
  const processMonitorServer = require('./process-monitor-server.service');
  const processService = require('./process.service');
  const serverStatusService = require('./server-status.service');
  const queueLockService = require('./queue-lock.service');

  let redisClient;

  const service = {
    setCustomProcessImpl,
    submitProcess: processMonitorClient.submitProcess,
    cancelProcesses: processMonitorClient.cancelProcesses,
    waitForCompletion: processMonitorClient.waitForCompletion,
    monitorQueue: processMonitorServer.monitorQueue,
    addProcessType: processMonitorServer.addProcessType,
    formatClusterStatus,
    getServerId: processMonitorServer.getServerId,
    getRedisClient
  };

  zervCore.addModule('Distrib', service);

  processService.setZervDependency(zervCore);
  processMonitorClient.setZervDependency(zervCore);
  processMonitorServer.setZervDependency(zervCore);
  queueLockService.setZervDependency(zervCore);

  module.exports = service;


  const stopLocalServer = zervCore.stopLocalServer;

  zervCore.stopLocalServer = async (delay, exitDelay = 0) => {
    let serverStatus = serverStatusService.findByServerId(service.getServerId());
    serverStatus.state = 'shutdown in progress';
    serverStatusService.updateOne(serverStatus, []);
    await stopLocalServer(delay);
    serverStatus = serverStatusService.findByServerId(service.getServerId());
    serverStatus.state = 'shutdown';
    serverStatusService.updateOne(serverStatus, []);
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
  let shuttingDown;
  zervCore.shutdown = async function shutdown(delayBeforeShuttingdown = 5, exitDelay = 5) {
    if (shuttingDown) {
      return;
    }
    shuttingDown = setTimeout(() => {
      const serverStatuses = serverStatusService.findAll();
      _.forEach(serverStatuses, (serverStatus) => {
        if (serverStatus.id !== zervCore.getServerId()) {
          zervSync.notifyCreation('cluster', 'SERVER_SHUTDOWN', {serverId: serverStatus.id}, {allServers: true});
        }
      });
    }, delayBeforeShuttingdown);
    zervCore.stopLocalServer(delayBeforeShuttingdown, false);
    let h;
    await new Promise((resolve) => {
      // check that all servers are down on timely basis
      h = setInterval(() => {
        const newServerStatuses = serverStatusService.findAll();
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

  // starting point needs to be revised
  function setCustomProcessImpl() {
    serverStatusService.listenToServerStatusData(zervCore);
  }

  /**
   * this returns a redis client.
   * This function should be in zerv-sync comming from the cluster source code which defines one already.
   *
   * @returns {IoRedis}
   */
  function getRedisClient() {
    if (!redisClient && process.env.REDIS_ENABLED === 'true') {
      // --------------------------
      // Later on, let's use instead the redis client initalized in zerv-sync/clustering
      // if there were any error, zerv-sync will throw errors anyway
      const connectionParams = {
        port: process.env.REDIS_PORT || 6379,
        host: process.env.REDIS_HOST || '127.0.0.1',
      };
      redisClient = new IoRedis(connectionParams);
      // --------------------------
    }
    return redisClient;
  }

  async function formatClusterStatus(findTenandByIdFn) {
    if (_.isEmpty(getRedisClient())) {
      return 'Redis client is disabled';
    }
    const tenantCache = {};
    const colors = {};
    colors[processService.PROCESS_STATUSES.SCHEDULED] = 'orange';
    colors[processService.PROCESS_STATUSES.IN_PROGRESS] = 'green';
    colors[processService.PROCESS_STATUSES.COMPLETE] = 'black';
    colors[processService.PROCESS_STATUSES.ERROR] = 'red';
    const serverStatuses = await serverStatusService.findAll();
    return (`<!doctype html>
    <html>
    <body>
    <h1>ZERV CONSOLE</h1>
    <h2>CLUSTER STATUS</h2>
    <table border="1" cellspacing="7" cellpadding="2" width="100%">${await getServerStatusRows()}</table>
    <h2>CLUSTER PROCESS QUEUE</h2>
    <table  cellspacing="7"  width="100%">${await getProcessRows()}</table>
    </body>
    </html>`);

    function findTenandById(tenantId) {
      let tenant = tenantCache[tenantId];
      if (!tenant) {
        tenant = tenantCache[tenantId] = findTenandByIdFn(tenantId);
      }
      return tenant;
    }

    async function getServerStatusRows() {
      const statuses = _.orderBy(serverStatuses, s => s.id);
      return (await Promise.all(_.map(statuses, formatServerStatus))).join('');
    }

    async function getProcessRows() {
      let processes = await processService.findAll();
      processes = _.orderBy(processes, p => -(new Date(p.createdDate).getTime()));
      processes = _.slice(processes, 0, 500);
      processes = await Promise.all(_.map(processes, formatProcess));
      return processes.join('');
    }

    async function formatProcess(p) {
      const color = _.get(colors, p.status, 'grey');
      const tenant = await findTenandById(p.tenantId);
      const serverStatus = _.find(serverStatuses, {id: p.serverId});
      let serverName;
      if (serverStatus && serverStatus.isAlive()) {
        serverName = serverStatus.getShortName();
      } else if (p.serverId) {
        // this server does not exist or has not signaled its presence to this server yet.
        serverName = 'offline server/' + p.serverId.slice(-8);
      } else {
        serverName = '';
      }
      return `
        <tr><td rowspan=2>${moment(p.createdDate).format('MMM Do, hh:mm:ss A')}</td><td rowspan=2>${p.type}<br/>${p.name}<br/><b>${tenant.display}</b><br/>${p.scheduledDate ? 'Scheduled on ' + moment(p.scheduledDate).format('MMM Do, hh:mm:ss A') : ''}</td><td><span style="color:${color};font-weight:bold">${p.status}</span> - ${p.start ? 'started at ' + moment(p.start).format('hh:mm:ss A') : 'Not started'}</td><td>${p.duration} seconds</td><td>${serverName}</td></tr>
        <tr><td colspan=3>${p.progressDescription || ''} ${p.error ? '<br/>Err: ' + (p.error.description || p.error.message) : ''} </td></tr>
        <tr><td colspan=5 style ="outline: thin solid"></tr>
        `;
    }

    async function formatServerStatus(s) {
      const processGroups = _.map(_.groupBy(s.activeProcesses, 'call'), (ps, group) =>
        `${group}(${ps.length})`
      );

      const userSessions = [];
      // NOTE: do not remove
      // the usersessions contains inactive sessions, is it really useful?
      // it indicates that the user has disconnected but not that he logged out.
      // if disconnected, the socket might reconnect anytime. it helps knowing
      // when the user logged in.
      // how to know if he did log out?
      const tenantGroups = _.groupBy(_.filter(s.userSessions, {active: true}), 'tenantId');
      for (const tenantId of _.keys(tenantGroups)) {
        const groupSessions = tenantGroups[tenantId];
        const tenant = await findTenandById(tenantId);
        const users = _.map(groupSessions, (s) => {
          const display = _.get(s.payload, 'display', s.id);
          return display + (s.active?'':'(inactive)');
        });
        userSessions.push(`${tenant.display}[${users}]`);
      }
      return `
        <tr><td rowspan=2 width="250px";>${s.getShortName()}<br/><br/>Version: <b>${s.appVersion}</b></td><td>Start: ${moment(s.start).format('MMM Do, hh:mm:ss A')}<br/>Last update: ${moment(s.alive).format('MMM Do, hh:mm:ss A')}</td><td>${s.getStateDisplay()}<br/>Running processes: ${s.activeProcesses.length}</td></tr>
        <tr><td colspan=2 height="40px"><b>Processes</b>: ${processGroups}<br\><b>Users</b>: ${userSessions}</td></tr>
      `;
    }
  }
}


