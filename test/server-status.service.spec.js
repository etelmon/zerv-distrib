const _ = require('lodash');
const zerv = require('zerv-core');
const moment = require('moment');

const service = require('../lib/server-status.service');

describe('server status service', () => {
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
        zerv.onChanges = _.noop;
        zerv.notifyCreation = _.noop;
        zerv.notifyUpdate = _.noop;
        zerv.getServerId = service.getServerId;
        spyOn(zerv, 'notifyCreation').and.callFake(notifyCreation);
        spyOn(zerv, 'notifyUpdate').and.callFake(notifyUpdate);


        onChanges = {};
        spyOn(zerv, 'onChanges').and.callFake((event, fn) => onChanges[event] = fn);
        service.setZervDependency(zerv);
    });


    function notifyCreation(tenantId, dataEvent, obj, options) {
        const fn = onChanges[dataEvent];
        fn && fn(tenantId, obj, 'create');
    }
    function notifyUpdate(tenantId, dataEvent, obj, options) {
        const fn = onChanges[dataEvent];
        fn && fn(tenantId, obj, 'update');
    }

    it('should launch scheduler to notify server status periodically', () => {
        spyOn(service, 'notifyServerStatusPeriodically');
        service.monitorServerStatus('ServerPlus', 'v3.12', { serverStayAliveInSecs: 10, serverStayAliveTimeoutInSecs: 30});
        expect(service.notifyServerStatusPeriodically).toHaveBeenCalled();
        expect(service.notifyServerStatusPeriodically).toHaveBeenCalledWith(
            'ServerPlus', 
            { appVersion: 'v3.12', serverStayAliveInSecs: 10, serverStayAliveTimeoutInSecs: 30 }
        );
    });

    it('should create a server status instance', () => {
        service.listenToServerStatusData(zerv);
        const serverStatus = service.createOne('superServer', 'appV1.2', 10);

        expect(serverStatus).toEqual(jasmine.objectContaining({
            id: jasmine.any(String),
            type: 'superServer',
            alive: new Date(),
            start: new Date(),
            revision: jasmine.any(Number),
            appVersion: 'appV1.2',
            activeProcesses: [],
            timeout: 10,
            state: 'up and running',
            userSessions: []
        }));

        expect(zerv.notifyCreation).toHaveBeenCalledTimes(1);

        const statuses = service.findAll();
        expect(statuses.length).toBe(1);

        expect(statuses[0]).not.toBe(serverStatus);
        expect(statuses[0]).toEqual(serverStatus);
    });

    it('should set the zerv id with the created server status instance', () => {
        const serverStatus = service.monitorServerStatus('superServer', 'appV1.2');
        expect(serverStatus.id).toEqual(zerv.getServerId());
    });

    it('should update a server status instance', () => {
        service.listenToServerStatusData(zerv);
        const serverStatus = service.createOne('superServer', 'appV1.2', 10);
        const previousServerStatus = _.clone(serverStatus);
        jasmine.clock().tick(2000);
        serverStatus.userSessions = [{ id: 'idU', display: 'User' }];

        service.updateOne(serverStatus);

        expect(serverStatus).toEqual(jasmine.objectContaining({
            id: jasmine.any(String),
            type: 'superServer',
            alive: new Date(),
            start: previousServerStatus.start,
            revision: previousServerStatus.revision + 1,
            appVersion: 'appV1.2',
            activeProcesses: [],
            timeout: 10,
            state: 'up and running',
            userSessions: [{ id: 'idU', display: 'User' }]
        }));

        expect(zerv.notifyUpdate).toHaveBeenCalledTimes(1);

        const statuses = service.findAll();
        expect(statuses.length).toBe(1);

        expect(statuses[0]).not.toBe(serverStatus);
        expect(statuses[0]).toEqual(serverStatus);
    });

    it('should add a new server to status list when notified', () => {
        service.listenToServerStatusData(zerv);
        const existingServerStatus = service.createOne('superServer', 'appV1.2', 10);

        const json = {
            id: 'someServerId',
            type: 'specializedServer',
            alive: new Date(),
            start: new Date(),
            revision: 1611550800000,
            appVersion: 'appV1.2',
            activeProcesses: [],
            timeout: 10,
            state: 'up and running',
            userSessions: [{ id: 'idU', display: 'User' }]
        };

        notifyUpdate('cluster', 'SERVER_STATUS_DATA', json);

        const NofifiedServerStatus = new service.ServerStatus(json);

        const statuses = service.findAll();
        expect(statuses.length).toBe(2);

        expect(statuses[0]).toEqual(existingServerStatus);
        expect(statuses[1]).not.toBe(NofifiedServerStatus);
        expect(statuses[1]).toEqual(NofifiedServerStatus);
    });

    describe('shutdown functionality', () => {
        let serverStatusJson;
        beforeEach(() => {
            zerv.stopLocalServer = _.noop;
            service.setZervDependency(zerv);


            const serverStatus = service.monitorServerStatus('superServer', 'appV1.2');
            serverStatusJson = JSON.parse(JSON.stringify(serverStatus));

            // status of a different server
            const server2Status = {
                id: 'someServerId',
                type: 'specializedServer',
                alive: new Date(),
                start: new Date(),
                revision: 1611550800000,
                appVersion: 'appV1.2',
                activeProcesses: [],
                timeout: 10,
                state: 'up and running',
                userSessions: [{ id: 'idU', display: 'User' }]
            };

            notifyUpdate('cluster', 'SERVER_STATUS_DATA', server2Status);
        });

        it('should request shutdown of the local zerv instance', () => {
            spyOn(zerv, 'stopLocalServer');
            notifyUpdate('cluster', 'SERVER_SHUTDOWN', {serverId: zerv.getServerId()});
            const statuses = service.findAll();
            expect(statuses.length).toBe(2);

            const expectedStatus = new service.ServerStatus(serverStatusJson);

            expectedStatus.state = 'request shutdown';
            expectedStatus.revision = expectedStatus.revision + 1;
            expect(statuses[0]).toEqual(expectedStatus);
        });
    });
});
