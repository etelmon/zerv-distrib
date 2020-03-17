const _ = require('lodash');
const zerv = require('zerv-core');
const processMonitorClientService = require('../lib/process-monitor-client.service');
const service = require('../lib/process.service');
const ioRedisLock = require('ioredis-lock');
const moment = require('moment');


describe('ProcessService', () => {
  let spec;
  let cache, locks;

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
    cache = {};
    locks = {};
    spec = {};
    spec.serverId1 = 'server1';
    spec.tenantId = 'tenantId1';
    spec.name = null;
    spec.type = 'doSomething';
    spec.type2 = 'doSomething2';
    spec.type3 = 'doSomething3ButFail';


    const mockRedis = {
      hset: (list, key, value) => {
        const queue = _.get(cache, list);
        if (!queue) {
          _.set(cache, list, [value]);
        } else {
          queue.push(value);
        }
      },
      hdel: (list, key) => {
        const queue = _.get(cache, list);
        if (queue) {
          _.remove(queue, (val) => JSON.parse(val).id === key);
        }
      },
      hvals: (list) => {
        return _.get(cache, list);
      },
      rpush: (list, value) => {
        mockRedis.hset(list, '', value);
      }
    };


    zerv.getRedisClient = () => mockRedis;
    zerv.onChanges = _.noop;
    zerv.notifyCreation = _.noop;
    zerv.notifyUpdate = _.noop;
    processMonitorClientService.setZervDependency(zerv);
    service.setZervDependency(zerv);

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


  describe('setPendingProcessToInProgress function', () => {
    it('should set the process to in progress', async () => {
      const p = await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {}, {});

      const process = await service.setPendingProcessToInProgress(p, spec.serverId1);
      expect(cache['zerv-pending-queue'].length).toBe(0);
      expect(cache['zerv-in-progress-queue'].length).toBe(1);
      expect(process).toEqual(jasmine.objectContaining({
        id: p.id,
        createdDate: jasmine.any(Date),
        lastModifiedDate: jasmine.any(Date),
        type: 'doSomething',
        name: null,
        tenantId: 'tenantId1',
        params: {},
        single: false,
        revision: 1,
        start: new Date,
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
});
