# zerv-distrib



### Scope
Simple process load distribution in a Zerv Server cluster to improve responsiveness of the main Zerv socket application servers.

This demonstrates that distributing processes in a Zerv environment is easily attainable and offers major performance improvement over the whole Zerv cluster in a few lines of code.

### pre-requisite
This relies on both zerv-core and zerv-sync libraries as well as redis to store the process queue.

### Principle
In order to free resources on the main socket servers (public facing application servers), zerv can easily distribute the load on the zerv cluster.
The main servers which the browser apps connect via the socket to execute api funtions and receive subscription data updates should always be responsive for a better user experience.
All processes that requires lots of processing or do not require an immediate response should be either:
- handled by other dedicated Zerv servers
- or main server should limit their number running at the same time to remain responsive.

A distributed environment also provide redundancy and can scale easily.


A process queue shares by the zerv cluster contains the processes to execute. 
When a decicated server is available, it will execute the process.
The server that initiates the process might wait for its completion as per requirements.

A process can use zerv notifications to communicate changes or return a result.

If server crashes during the process, another server will restart it or when the crashed server restarts.

### Example

On the server that needs to initiate a process thru a queue, create a function that submit a process and that could even wait for its completion.

```javascript
async function requestSfPermissionUpdate(tenantId, user) {

    const process = await zerv.submitProcess(tenantId, 'UpdateSfPermission', `tenant${tenantId}/${user.id}`,{tenantId, user});
    // check if we need to wait (if there is no permission let's wait for process to complete)
    // otherwise use the permission we already know then the new ones will be pushed over the network
    if (! await opportunityPermissionService.hasOpportunityPermissions(tenantId, user) ) {
        return zerv.waitForCompletion(process)
        .then(()=> 'done')
        .catch(err => {
            console.error(err);
            throw err;
        })
    }
    return 'started';
}
```

On the server that will consume this process (it could be the same as the server requester), you would need the following code to be run at the server launch.
Then the server will monitor the queur.

```javascript
function monitorQueue() {
    zerv.addProcessType('UpdateSfPermission', sfPermisionService.updateOpportunityPermissions, {
        gracePeriodInMins: 5, // if in 5 minutes the process did not come back, it must be crashed. it will restart by itself
    });
    // this is a requirement to check the server statuses before monitoring the queue
    zerv.monitorServerStatus('aPermissionServer', 'your code base version');

    zerv.monitorQueue(process.env.MAX_CAPACITY || 5);
}


function updateOpportunityPermissions(tenantId, processHandle, params) {
    processHandle.setProgressDescription(`working on the request sent by ${params.user}`);
    ...
    // when completed, the process must return the following params.
    return {
        data: null,  // here data could be returned if needed by the caller (means it is waiting for it)
        description: 'the process completed as expected' // describe how the process completed for logging purposes
    }
}
```

### Api

__monitorServerStatus(serverName, appVersion, options)__

{String} serverName is the type of server so that it can be identified in the cluster. 

{String} appVersion is the code base version for log purposes.

{object} options
- {Number} serverStayAliveInSecs how often the server notifies that it is alive
- {Number} serverStayAliveTimeoutInSecs how long before server is considered offline. By default 4*serverStayAliveInSecs


This function listens to the server status that is notified by other servers fo the cluster and commands to shutdown.
It also notifies periodically this local server server status to the cluster.

The server status contains server identity, current activities in progress, and user sessions.


__monitorQueue(capacity)__

This function launches the monitoring of the process queue by the current server.

The function monitorServerStatus must be called first in order to:
- identify server owner of processes 
- and detect if processes are stalled due to down servers.

Provide any port the server might be listening to.

capacity is key. the algorithm is currently simple. It limits the number of processes run by the server.
It depends on the server physical capacity. If the number is too high, the server could become unresponsive and take a while to recover.

__addProcessType(name, processImplementation, options)__

{String} name: It is the name of the process handles by the server that was submitted by submitProcess function

{Function} processImplementation: It provides the how process is implemented by the server that will be monitoring the queue.

the processImplementation receives an handle.
the handle is practical to update the process status visible in logs (setProgressDescription) and to test if the server is shutting down.

the process implementation must return an object with the following properties 
- {Object} data: data to return to the requester
- {String} description: message about its completion to show in logs.

{Object} options:
- {Number} gracePeriodInMins: Be careful to provide a value high enough otherwise the server could restart the process before its completion, which could lead to saturating the server.
- {Number} wasteTimeInSecs: This will add a duration to a process execution useful to simulate/test more concurrency
- {Number} priority: When multiple processes are in the queue, the highest priority will executed first. by default priority is 10.
 

__submitProcess(tenantId, type, name, params, options)__

This function submits a new process with its parameters. The type must be declared by a monitoring server otherwise the process will never get executed.

{String} tenantId: the id of the tenant executing the process

{String} type: the name of the process type handled by a server (addProcessType)

{String} name: it is a uniq name/hashkey for this process type to identify different processes of the same types. mostly help for logs

{Object} options: 
- {Boolean} single: If single is true and another process with the same name is submitted again while the first one is not completed, no other process is created. The existing process is actually returned. By default true.

__waitForCompletion(process, timeoutInSecs)__

This function waits for the completion of a process.

__shutdown(delay)__

The function executes a safe and graceful shutdown of the whole zerv cluster.
It will exit all zerv instance nodes when all activities (api calls or started processes) completed and will not execute any further activity.
```javascript
zerv.shutdown(10);
```

### To Implement

- implement shutdownServer of a specific server remotely in addition to the existing shutdown function
- Should allow custom load balancing strategies (ex based on tenant restrictions, one tenant could have more allocated slot to run processes than another)
- large result should not be broadcasted but store in redis, and process should return a cursor id (similar to SF)
- have an option to restart a process a limited number of times if it crashes. Currently it will keep retrying. In theory developer should cache all exceptions in their implemented process. On the other hand, the infrastructure should restart down servers so there is little chance to go to infinite loop.
- Implement scheduler, and process should be able to decide if it should restart or not.
- Add option to distribute zerv api server apis (api route can be easily distributed)
- Add ability to change zerv worker capacity in real time
- create a real time zerv monitor of the processes distribution (relies on subscription)
- reduce notification amount data broadcasted thru redis on a regular basis (granular notifications)
