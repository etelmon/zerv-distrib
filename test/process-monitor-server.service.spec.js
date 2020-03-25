const _ = require('lodash');
const moment = require('moment');

const zerv = require('zerv-core');
const service = require('../lib/process-monitor-server.service');
const processMonitorClientService = require('../lib/process-monitor-client.service');
const queueProcessService = require('../lib/queue-process.service');
const ioRedisLock = require('ioredis-lock');
const RedisClientMock= require('./redis-client-mock');
const serverStatusService = require('../lib/server-status.service');


describe('ProcessMonitorServerService', () => {
    let spec;
    let locks, redisMock;
    beforeEach(() => {
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
        queueProcessService.setZervDependency(zerv);
        service.setZervDependency(zerv);

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

        spyOn(serverStatusService, 'getServerId').and.returnValue(spec.serverId1);
    });


    describe('monitorQueue function', () => {

        beforeEach(() => {
            spyOn(serverStatusService, 'createOne');
            spyOn(service,'_listenProcessQueueForNewRequests');
            spyOn(serverStatusService, 'notifyServerStatusPeriodically');
        });

        it('should start monitoring the queue when capacity profile is provided', () => {
            service.monitorQueue(10);
            expect(service._listenProcessQueueForNewRequests).toHaveBeenCalled();
        });
        it('should not monitor the queue when capacity profile is 0', () => {
            service.monitorQueue(0);
            expect(service._listenProcessQueueForNewRequests).not.toHaveBeenCalled();
        });
    });

    describe('_selectNextProcessesToRun function', () => {
        it('should pick the submitted process in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});

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
            service._setCapacityProfile(2);


            const next = await service._selectNextProcessesToRun();
            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should pick the 2 submitted process in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();

            expect(next.length).toEqual(2);

            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
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
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should pick only 2 submitted process among the 4 in the queue', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type2, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();

            expect(next.length).toEqual(2);

            expect(next[0]).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
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
                single: true,
                revision: 1,
                start: jasmine.any(Date),
                end: null,
                status: 'in progress',
                progressDescription: 'Server server1 will execute this process',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });
    });

    describe('_executeProcess function', () => {
        it('should complete the process successfully', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();
            const process = await service._executeProcess(next[0]);

            expect(process).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 3,
                start: jasmine.any(Date),
                end: jasmine.any(Date),
                status: 'complete',
                progressDescription: 'done',
                serverId: 'server1',
                data: null,
                error: null
            }));
        });

        it('should fail to complete the process', async () => {
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type3, spec.name, {});
            service._setCapacityProfile(2);
            const next = await service._selectNextProcessesToRun();
            const process = await service._executeProcess(next[0]);

            expect(process).toEqual(jasmine.objectContaining({
                id: jasmine.any(String),
                createdDate: jasmine.any(Date),
                lastModifiedDate: jasmine.any(Date),
                type: 'doSomething3ButFail',
                name: null,
                tenantId: 'tenantId1',
                params: {},
                single: true,
                revision: 3,
                start: jasmine.any(Date),
                end: jasmine.any(Date),
                status: 'error',
                progressDescription: 'Started by server server1',
                serverId: 'server1',
                data: null,
                error: {message: 'FAIL', description: undefined}
            }));
        });
    });

    xdescribe('_checkIfProcessIsNotStalled', () => {
        it('should not throw any error as process is valid', () => {

        });
        it('should throw an error when process gracePeriod is over', () => {

        });
        it('should throw an error when process is not handled by the server owner', () => {

        });

    });

    describe('_runNextProcesses function', () => {

        beforeEach(() => {
            service._setCapacityProfile(2);
            spyOn(service, '_scheduleToCheckForNewProcessResquests');
            spyOn(service,'_runNextProcesses').and.callThrough();
        });

        it('should tract the process execution in the server active process map to track the load', async () => {
            spyOn(service, '_executeProcess').and.callFake(() => {
                expect(service._getActiveProcesses().length).toBe(1);
                return 'completed way or another';
            });

            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});

            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
        });

        it('should indicate that the queue is being processed to reduce access to the queue', async () => {
            expect(service._waitForProcessingQueue).toBeNull();
            const promise = service._runNextProcesses();
            expect(service._waitForProcessingQueue).not.toBeNull();
            expect(service._waitForProcessingQueue).toEqual(jasmine.any(Promise));
            await promise;
            expect(service._waitForProcessingQueue).toBeNull();
        });

        it('should not retry to check for new processes since the trigger is a notification', async () => {
            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
            expect(service._scheduleToCheckForNewProcessResquests).not.toHaveBeenCalled();     
        });

        it('should schedule and check the queue as soon as a process completes', async () => {
            spyOn(service, '_executeProcess').and.returnValue('completed way or another');
            service._scheduleToCheckForNewProcessResquests.and.callThrough();
            await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
      
            expect(service._getActiveProcesses().length).toBe(0);
            await service._runNextProcesses();
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(1);
            expect(service._runNextProcesses).toHaveBeenCalledTimes(2);
        });

    });

    describe('_listenProcessQueueForNewRequests function', () => {

        let onChanges;

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
            onChanges = {};
            spyOn(service, '_scheduleToCheckForNewProcessResquests');
            spyOn(zerv, 'notifyCreation').and.callFake(notifyCreation);
            spyOn(zerv, 'notifyUpdate').and.callFake(notifyUpdate);
            spyOn(zerv, 'onChanges').and.callFake((event, fn) => onChanges[event] = fn);
        });

        it('should launch the processing of the queue in a few seconds', async () => {
            service._setCapacityProfile(2);
            service._listenProcessQueueForNewRequests();
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(0);
            jasmine.clock().tick(5*1000);
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(1);
        });

        it('should launch the processing of the queue on a regular basis', async () => {
            service._setCapacityProfile(2);
            service._listenProcessQueueForNewRequests();
            jasmine.clock().tick(5*1000);
            jasmine.clock().tick(60*1000);
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(2);
            jasmine.clock().tick(60*1000);
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(3);
            jasmine.clock().tick(60*1000);
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(4);

        });

        it('should launch the processing of the queue when a new process is posted (status pending)', async () => {
            service._setCapacityProfile(2);
            service._listenProcessQueueForNewRequests();
            const process = await processMonitorClientService.submitProcess(spec.tenantId, spec.type, spec.name, {});
            expect(process.status).toBe('pending');
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(1);

        });

        it('should NOT launch the processing of the queue when a process is updated', () => {
            service._setCapacityProfile(2);
            service._listenProcessQueueForNewRequests();
            notifyUpdate(spec.tenantId, 'TENANT_PROCESS_DATA', {id:'someprocessId', status: 'in progress'});
            expect(service._scheduleToCheckForNewProcessResquests).toHaveBeenCalledTimes(0);
        });

        function notifyCreation(tenantId, dataEvent, obj, options) {
            const fn = onChanges[dataEvent];
            fn && fn(tenantId, obj, 'create');
        }
        function notifyUpdate(tenantId, dataEvent, obj, options) {
            const fn = onChanges[dataEvent];
            fn && fn(tenantId, obj, 'update');
        }
    });

    describe('_scheduleToCheckForNewProcessResquests function', () => {
        it('should launch the processing of the queue if the queue is not being processed', () => {
            spyOn(service,'_runNextProcesses');
            service._waitForProcessingQueue = null;
            service._scheduleToCheckForNewProcessResquests();
            expect(service._runNextProcesses).toHaveBeenCalled();
        });

        it('should NOT launch the processing of the queue again since _waitForProcessingQueue was set', async () => {
            spyOn(service,'_runNextProcesses');
            let waitDone;
            service._waitForProcessingQueue = new Promise((resolve) => waitDone = resolve);
            const promise = service._scheduleToCheckForNewProcessResquests();
            expect(service._runNextProcesses).not.toHaveBeenCalled();
            waitDone();

            // queue is already being processed
            service._waitForProcessingQueue = Promise.resolve();
            // not need to check the queue
            await promise;
            expect(service._runNextProcesses).toHaveBeenCalledTimes(0);
        });

        it('should launch the processing of the queue again since _waitForProcessingQueue was reset', async () => {
            spyOn(service,'_runNextProcesses');
            let waitDone;
            service._waitForProcessingQueue = new Promise((resolve) => waitDone = resolve);
            const promise = service._scheduleToCheckForNewProcessResquests();
            expect(service._runNextProcesses).not.toHaveBeenCalled();
            waitDone();

            // queue is not currently in process
            service._waitForProcessingQueue = null;
            // need to check the queue again (means processes were received by not handled during )
            await promise;
            expect(service._runNextProcesses).toHaveBeenCalledTimes(1);
        });

    });


});


