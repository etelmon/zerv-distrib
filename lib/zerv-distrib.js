
'strict mode';

const _ = require('lodash');

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
    const queueProcessService = require('./queue-process.service');
    const serverStatusService = require('./server-status.service');
    const serverConsoleService = require('./server-console.service');

    let redisClient;

    const service = {
        monitorServerStatus: serverStatusService.monitorServerStatus,
    
        submitProcess: processMonitorClient.submitProcess,
        waitForCompletion: processMonitorClient.waitForCompletion,

        addProcessType: processMonitorServer.addProcessType,
        monitorQueue: processMonitorServer.monitorQueue,
       
        formatClusterStatus,
        getServerId: processMonitorServer.getServerId,

        stopLocalServer: serverStatusService.stopLocalServer,
        shutdown: serverStatusService.shutdownAllServers,

        getRedisClient
    };

    // Zerv is augmented with all those functions
    zervCore.addModule('Distrib', service);

    serverStatusService.setZervDependency(zervCore);
    queueProcessService.setZervDependency(zervCore);
    processMonitorClient.setZervDependency(zervCore);
    processMonitorServer.setZervDependency(zervCore);

    module.exports = service;


  /**
   * this returns a redis client.
   * This function should be in zerv-sync comming from the cluster source code which declares it already.
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

    function formatClusterStatus(findTenantByIdFn) {
        if (_.isEmpty(getRedisClient())) {
            return 'Redis client is disabled';
        };
        return serverConsoleService.getHtmlReport(findTenantByIdFn);
    }
}
