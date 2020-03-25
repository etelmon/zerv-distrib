const _ = require('lodash');
const zerv = require('zerv-core');
const service = require('../lib/queue-process.service');
const moment = require('moment');
const RedisClientMock = require('./redis-client-mock');


describe('ProcessService', () => {
    let redisMock;
    const serverId1 = 'server1';
    const tenantId = 'tenantId1';

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
        redisMock = new RedisClientMock();
        zerv.getRedisClient = () => redisMock;

        zerv.onChanges = _.noop;
        zerv.notifyCreation = _.noop;
        zerv.notifyUpdate = _.noop;

        spyOn(zerv, 'notifyCreation');
        spyOn(zerv, 'notifyUpdate');


        service.setZervDependency(zerv);
    });


    it('createOne function should set the process to in progress', async () => {
        const newProcess = new service.QueueProcess({
            type: 'doSomething',
            name: null,
            tenantId: 'tenantId1',
            params: {},
            single: true,
            status: 'pending'
        });
        await service.createOne(tenantId, newProcess);

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

        expect(zerv.notifyCreation).toHaveBeenCalledTimes(1);
    });

    describe('updateOne function', () => {
        let process;

        beforeEach(async () => {
            process = await service.createOne(
                tenantId,
                new service.QueueProcess({
                    type: 'doSomething',
                    name: null,
                    tenantId: 'tenantId1',
                    params: {},
                    single: true,
                    status: 'pending'
                })
            );
        });
        it('should update pending process to in progress in the active queue', async () => {
            process.start = new Date();
            process.status = service.PROCESS_STATUSES.IN_PROGRESS;
            process.serverId = serverId1;

            await service.updateOne(tenantId, process);
            expect(process.revision).toEqual(1);

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
                revision: 1,
                start: jasmine.any(String),
                end: null,
                status: 'in progress',
                progressDescription: null,
                serverId: 'server1',
                data: null,
                error: null
            });

            expect(zerv.notifyUpdate).toHaveBeenCalledTimes(1);
        });

        it('should update in progress process to complete and move it from the active queue to inactive storage', async () => {
            process.start = new Date();
            process.status = service.PROCESS_STATUSES.IN_PROGRESS;
            process.serverId = serverId1;

            await service.updateOne(tenantId, process);

            process.end = new Date();
            process.status = service.PROCESS_STATUSES.COMPLETE;

            await service.updateOne(tenantId, process);
            expect(process.revision).toEqual(2);

            expect(redisMock.hvals('zerv-active-queue').length).toEqual(0);

            expect(redisMock.cache['zerv-inactive-queue']).toBeDefined();
            const cacheData = redisMock.lrange('zerv-inactive-queue');
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
                revision: 2,
                start: jasmine.any(String),
                end: jasmine.any(String),
                status: 'complete',
                progressDescription: null,
                serverId: 'server1',
                data: null,
                error: null
            });

            expect(zerv.notifyUpdate).toHaveBeenCalledTimes(2);
        });
    });
});
