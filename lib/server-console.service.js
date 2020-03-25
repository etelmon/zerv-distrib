
'strict mode';

const _ = require('lodash');
const moment = require('moment');

const queueProcessService = require('./queue-process.service');
const serverStatusService = require('./server-status.service');

module.exports = {
    getHtmlReport
};

/**
 * a quick output to check the queue and server statuses.
 * 
 * @param {Function} findTenandByIdFn function to retrieve the tenant name for display
 */
async function getHtmlReport(findTenandByIdFn) {
    const tenantCache = {};
    const colors = {};
    colors[queueProcessService.PROCESS_STATUSES.IN_PROGRESS] = 'green';
    colors[queueProcessService.PROCESS_STATUSES.COMPLETE] = 'black';
    colors[queueProcessService.PROCESS_STATUSES.ERROR] = 'red';
    const serverStatuses = await serverStatusService.findAll();
    return (`<!doctype html>
    <html>
    <body>
    <h1>ZERV CONSOLE</h1>
    <h2>CLUSTER STATUS</h2>
    <table border="1" cellpadding="7" width="100%">${await getServerStatusRows()}</table>
    <h2>CLUSTER PROCESS QUEUE</h2>
    <table border="0" cellpadding="7"  width="100%">${await getProcessRows()}</table>
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
        let processes = await queueProcessService.findAll();
        processes = _.orderBy(processes, p => -(new Date(p.createdDate).getTime()));
        processes = _.slice(processes, 0, 500);
        processes = await Promise.all(_.map(processes, formatProcess));
        return processes.join('');
    }

    async function formatProcess(p) {
        const color = _.get(colors, p.status, 'grey');
        const tenant = await findTenandById(p.tenantId);
        const serverStatus = _.find(serverStatuses, { id: p.serverId });
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
        <tr><td rowspan=2>${moment(p.createdDate).format('MMM Do, hh:mm:ss A')}</td><td rowspan=2>${p.type}<br/>${p.name}<br/><b>${tenant.display}</b></td><td><span style="color:${color};font-weight:bold">${p.status}</span> - ${p.end ? 'ended at ' + moment(p.end).format('hh:mm:ss A') : ''}</td><td>${p.duration} seconds</td><td>${serverName}</td></tr>
        <tr style="-webkit-box-shadow: 0px 1px 0px 0px lightgray; -moz-box-shadow: 0px 1px 0px 0px lightgray; box-shadow: 0px 1px 0px 0px lightgray;"><td colspan=3>${p.progressDescription || ''} ${p.error ? '<br/>Err: ' + (p.error.description || p.error.message || p.error) : ''} </td></tr>
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
        const tenantGroups = _.groupBy(_.filter(s.userSessions, { active: true }), 'tenantId');
        for (const tenantId of _.keys(tenantGroups)) {
            const groupSessions = tenantGroups[tenantId];
            const tenant = await findTenandById(tenantId);
            const users = _.map(groupSessions, (s) => {
                const display = _.get(s.payload, 'display', s.id);
                return display + (s.active ? '' : '(inactive)');
            });
            userSessions.push(`${tenant.display}[${users}]`);
        }
        return `
        <tr><td rowspan=2 width="250px";>${s.getShortName()}<br/><br/>Version: <b>${s.appVersion}</b></td><td>Start: ${moment(s.start).format('MMM Do, hh:mm:ss A')}<br/>Last update: ${moment(s.alive).format('MMM Do, hh:mm:ss A')}</td><td>${s.getStateDisplay()}<br/>Running processes: ${s.activeProcesses.length}</td></tr>
        <tr><td colspan=2 height="40px"><b>Processes</b>: ${processGroups}<br\><b>Users</b>: ${userSessions}</td></tr>
      `;
    }
}
