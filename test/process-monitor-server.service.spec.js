const _ = require('lodash');
const zerv = require('zerv-core');
const service = require('../lib/process-monitor-server.service');
const processMonitorClientService = require('../lib/process-monitor-client.service');
const processService = require('../lib/process.service');
const ioRedisLock = require('ioredis-lock');
const queueLockService = require('../lib/queue-lock.service');
const RedisClientMock= require('./redis-client-mock');


describe('ProcessMonitorServerService', () => {
  let spec;
  let locks, redisMock;
  beforeEach(() => {
    cache = {};
    locks = {};
    spec = {};
    spec.serverId1 = 'server1';
    spec.tenantId = 'tenantId1';
    spec.type = 'doSomething';
    spec.type2 = 'doSomething2';
    spec.type3 = 'doSomething3ButFail';

    redisMock = new RedisClientMock();
    zerv.getRedisClient = () => redisMock;

    zerv.onChanges = _.noop;
    zerv.notifyCreation = _.noop;
    zerv.notifyUpdate = _.noop;
    processMonitorClientService.setZervDependency(zerv);
    processService.setZervDependency(zerv);
    service.setZervDependency(zerv);
    queueLockService.setZervDependency(zerv);

    service.addProcessType(spec.type, (tenantId, processHandle, params) => ({
      description: 'done',
      data: null
    }));

    service.addProcessType(spec.type2, (tenantId, processHandle, params) => ({
      description: 'done',
      data: null
    }));

    service.addProcessType(spec.type3, (tenantId, processHandle, params) => {
      throw new Error('FAIL');
    });


    spyOn(ioRedisLock, 'createLock').and.callFake(() => {
      let lock;
      const p = new Promise((resolve) => {
        releaseLock = resolve;
      });
      return {
        acquire: async (lockName) => {
          lock = lockName;
          const previous = locks[lock];
          if (previous) {
            await previous;
          }
          locks[lock] = p;
        },
        release: () => {
          delete locks[lock];
          releaseLock();
        }
      };
    });
  });

  describe('_selectNextProcessesToRun function', () => {
    it('should pick the submitted process in the queue', async () => {
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      expect(redisMock.cache['zerv-pending-queue']).toBeDefined();

      const r = JSON.parse(redisMock.cache['zerv-pending-queue'][0]);
      expect(r).toEqual({
        id: jasmine.any(String),
        createdDate: jasmine.any(String),
        lastModifiedDate: jasmine.any(String),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 0,
        start: null,
        end: null,
        status: 'pending',
        scheduledDate: null,
        progressDescription: 'Pending',
        serverId: null,
        data: null,
        error: null
      });
      service._setCapacityProfile(2);


      const next = await service._selectNextProcessesToRun(spec.serverId1);
      expect(next[0]).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: jasmine.any(Date),
        end: null,
        status: 'in progress',
        scheduledDate: null,
        progressDescription: 'Server server1 will execute this process',
        serverId: 'server1',
        data: null,
        error: null
      }));
    });

    it('should pick the 2 submitted process in the queue', async () => {
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      service._setCapacityProfile(2);
      const next = await service._selectNextProcessesToRun(spec.serverId1);

      expect(next.length).toEqual(2);

      expect(next[0]).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: jasmine.any(Date),
        end: null,
        status: 'in progress',
        scheduledDate: null,
        progressDescription: 'Server server1 will execute this process',
        serverId: 'server1',
        data: null,
        error: null
      }));
      expect(next[1]).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething2',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: jasmine.any(Date),
        end: null,
        status: 'in progress',
        scheduledDate: null,
        progressDescription: 'Server server1 will execute this process',
        serverId: 'server1',
        data: null,
        error: null
      }));
    });

    it('should pick only 2 submitted process among the 4 in the queue', async () => {
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      service._setCapacityProfile(2);
      const next = await service._selectNextProcessesToRun(spec.serverId1);

      expect(next.length).toEqual(2);

      expect(next[0]).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: jasmine.any(Date),
        end: null,
        status: 'in progress',
        scheduledDate: null,
        progressDescription: 'Server server1 will execute this process',
        serverId: 'server1',
        data: null,
        error: null
      }));
      expect(next[1]).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething2',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: jasmine.any(Date),
        end: null,
        status: 'in progress',
        scheduledDate: null,
        progressDescription: 'Server server1 will execute this process',
        serverId: 'server1',
        data: null,
        error: null
      }));
    });
  });

  describe('_executeProcess function', () => {
    it('should complete the process successfully', async () => {
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      service._setCapacityProfile(2);
      const next = await service._selectNextProcessesToRun(spec.serverId1);
      const process = await service._executeProcess(next[0], spec.serverId1);

      expect(process).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 3,
        start: jasmine.any(Date),
        end: jasmine.any(Date),
        status: 'complete',
        scheduledDate: null,
        progressDescription: 'done',
        serverId: 'server1',
        data: null,
        error: null
      }));
    });

    it('should fail to complete the process', async () => {
      await processMonitorClientService.submitProcess(spec.tenantId, spec.type3, spec.name, {}, {});
      service._setCapacityProfile(2);
      const next = await service._selectNextProcessesToRun(spec.serverId1);
      const process = await service._executeProcess(next[0], spec.serverId1);

      expect(process).toEqual(jasmine.objectContaining({
        id: jasmine.any(String),
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething3ButFail',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 3,
        start: jasmine.any(Date),
        end: jasmine.any(Date),
        status: 'error',
        scheduledDate: null,
        progressDescription: 'Started by server server1',
        serverId: 'server1',
        data: null,
        error: {message: 'FAIL', description: undefined}
      }));
    });
  });
});
