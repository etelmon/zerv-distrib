const _ = require('lodash');
const zerv = require('zerv-core');
const service = require('../lib/process-monitor-client.service');
const queueProcessService = require('../lib/queue-process.service');
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
        queueProcessService.setZervDependency(zerv);

        spyOn(ioRedisLock, 'createLock').and.callFake(() => {
            let lock, releaseLock;
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
            await service.submitProcess(spec.tenantId, spec.type, spec.name, {});
            expect(redisMock.cache['zerv-active-queue']).toBeDefined();
            const cacheData = redisMock.hvals('zerv-active-queue');
            expect(cacheData.length).toEqual(1);

            const r = JSON.parse(cacheData[0]);
            expect(r).toEqual({
                id: jasmine.any(String),
                createdDate: jasmine.any(String),
                lastModifiedDate: jasmine.any(String),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 0,
                start: null,
                end: null,
                status: 'pending',
                progressDescription: null,
                serverId: null,
                data: null,
                error: null
            });
        });

        it('should submit 2 different process requests to the queue', async () => {
            await service.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await service.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            const cacheData = redisMock.hvals('zerv-active-queue');
            expect(cacheData.length).toEqual(2);


            let r = JSON.parse(cacheData[0]);
            expect(r).toEqual({
                id: jasmine.any(String),
                createdDate: jasmine.any(String),
                lastModifiedDate: jasmine.any(String),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 0,
                start: null,
                end: null,
                status: 'pending',
                progressDescription: null,
                serverId: null,
                data: null,
                error: null
            });
            r = JSON.parse(cacheData[1]);
            expect(r).toEqual({
                id: jasmine.any(String),
                createdDate: jasmine.any(String),
                lastModifiedDate: jasmine.any(String),
                type: 'doSomething2',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 0,
                start: null,
                end: null,
                status: 'pending',
                progressDescription: null,
                serverId: null,
                data: null,
                error: null
            });
        });

        it('should submit 2 similar process requests to the queue but prevent the 3rd one when single option is true', async () => {
            const p1 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await service.submitProcess(spec.tenantId, spec.type, spec.name, {}, { single: false });
            const p3 = await service.submitProcess(spec.tenantId, spec.type, spec.name, {});
            const cacheData = redisMock.hvals('zerv-active-queue');
            expect(cacheData.length).toEqual(2);
            expect(p3).toEqual(p1);
        });
    });
});
