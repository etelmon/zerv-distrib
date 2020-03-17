const _ = require('lodash');
const zerv = require('zerv-core');
const service = require('../lib/process-monitor-client.service');
const processService = require('../lib/process.service');
const queueLockService = require('../lib/queue-lock.service');
const ioRedisLock = require('ioredis-lock');
const moment = require('moment');
const RedisClientMock= require('./redis-client-mock');


describe('ProcessMonitorClientService', () => {
  let spec;
  let locks, redisMock;

  beforeEach(() => {
    jasmine.clock().uninstall();
    jasmine.clock().install();
    const baseTime = moment().year(2020).month(12).date(25);
    baseTime.startOf('day');
    jasmine.clock().mockDate(baseTime.toDate());
  });

  afterEach(() => {
    jasmine.clock().uninstall();
  });

  beforeEach(() => {
    locks = {};
    spec = {};
    spec.serverId1 = 'server1';
    spec.tenantId = 'tenantId1';
    spec.name = null;
    spec.type = 'doSomething';
    spec.type2 = 'doSomething2';
    spec.type3 = 'doSomething3ButFail';

    redisMock = new RedisClientMock();
    zerv.getRedisClient = () => redisMock;

    zerv.onChanges = _.noop;
    zerv.notifyCreation = _.noop;
    zerv.notifyUpdate = _.noop;
    service.setZervDependency(zerv);
    processService.setZervDependency(zerv);
    queueLockService.setZervDependency(zerv);

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

  describe('submitProcess function', () => {
    it('should submit a process request to the queue', async () => {
      await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      expect(redisMock.cache['zerv-pending-queue']).toBeDefined();
      expect(redisMock.cache['zerv-pending-queue'].length).toEqual(1);

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
    });

    it('should submit 2 different process requests to the queue', async () => {
      await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await service.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      expect(redisMock.cache['zerv-pending-queue'].length).toEqual(2);

      let r = JSON.parse(redisMock.cache['zerv-pending-queue'][0]);
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
      r = JSON.parse(redisMock.cache['zerv-pending-queue'][1]);
      expect(r).toEqual({
        id: jasmine.any(String),
        createdDate: jasmine.any(String),
        lastModifiedDate: jasmine.any(String),
        type: 'doSomething2',
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
    });

    it('should submit 2 similar process requests to the queue but prevent the 3rd one when single option is true', async () => {
      const p1 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      const p3 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, { single: true });
      expect(redisMock.cache['zerv-pending-queue'].length).toEqual(2);
      expect(p3).toEqual(p1);
    });

    it('should submit a scheduled process requests to the queue', async () => {
      await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, { scheduledDate: moment().year(2020).month(02).date(24).toDate() });
      expect(redisMock.cache['zerv-pending-queue']).toBeUndefined();
      expect(redisMock.cache['zerv-scheduled-queue'].length).toEqual(1);

      const r = JSON.parse(redisMock.cache['zerv-scheduled-queue'][0]);

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
        status: 'scheduled',
        scheduledDate: '2020-03-24T04:00:00.000Z',
        progressDescription: 'Scheduled start on Mar 24th, 12:00:00 AM',
        serverId: null,
        data: null,
        error: null
      });
    });
  });
  describe('cancelProcesses function', () => {
    it('should cancel same process requests pending in the queue', async () => {
      const p1 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      await service.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      const p3 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      expect(redisMock.cache['zerv-pending-queue'].length).toBe(3);

      const processes = await service.cancelProcesses(spec.tenantId, spec.type, spec.name);
      expect(redisMock.cache['zerv-pending-queue'].length).toBe(1);
      expect(processes.length).toBe(2);
      expect(processes[0]).toEqual(jasmine.objectContaining({
        id: p1.id,
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: null,
        end: null,
        status: 'canceled',
        scheduledDate: null,
        progressDescription: 'Canceled on Jan 25th, 12:00:00 AM',
        serverId: null,
        data: null,
        error: null
      }));
      expect(processes[1]).toEqual(jasmine.objectContaining({
        id: p3.id,
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: null,
        end: null,
        status: 'canceled',
        scheduledDate: null,
        progressDescription: 'Canceled on Jan 25th, 12:00:00 AM',
        serverId: null,
        data: null,
        error: null
      }));
    });

    it('should cancel scheduled process request scheduled in the queue', async () => {
      const p1 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, { scheduledDate: new Date() });
      await service.submitProcess(spec.tenantId, spec.type2, spec.name, {}, {});
      const p3 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});
      expect(redisMock.cache['zerv-pending-queue'].length).toBe(2);

      const processes = await service.cancelProcesses(spec.tenantId, spec.type, spec.name);
      expect(redisMock.cache['zerv-pending-queue'].length).toBe(1);
      expect(processes.length).toBe(2);
      expect(processes[0]).toEqual(jasmine.objectContaining({
        id: p3.id,
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: null,
        end: null,
        status: 'canceled',
        scheduledDate: null,
        progressDescription: 'Canceled on Jan 25th, 12:00:00 AM',
        serverId: null,
        data: null,
        error: null
      }));
      expect(processes[1]).toEqual(jasmine.objectContaining({
        id: p1.id,
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: null,
        end: null,
        status: 'canceled',
        scheduledDate: new Date(),
        progressDescription: 'Canceled on Jan 25th, 12:00:00 AM',
        serverId: null,
        data: null,
        error: null
      }));
    });
  });
});
