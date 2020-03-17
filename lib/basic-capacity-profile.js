const _ = require('lodash');
const assert = require('assert');

class BasicCapacityProfile {
  constructor(maxProcesses) {
    this.maxProcesses = maxProcesses;
    assert(maxProcesses>=0 && maxProcesses<10000, 'The capacity is invalid');
  }

  setActiveProcesses(activeProcesses) {
    this.activeProcesses = activeProcesses;
  }

  toString() {
    return this.maxProcesses;
  }

  /**
     * This function should decide if this server has 0 availability
     * to take any other processes in.
     *
     */
  isServerAtFullCapacity() {
    // Server could have a different capacity for long processes
    // otherwise short process might take time before being executed.
    return _.keys(this.activeProcesses).length >= this.maxProcesses;
  }

  /**
     * By default, the queue has a chronological order based on process created date and status
     * so that stalled processes show up first to be reprocessed asap
     * Scheduled processed are placed down the queue.
     *
     * order queue should provided by capacity profile which would offer different strategies to handle a queue
     *
     *
     * @param {Array} queue
     */
  orderQueue(queue) {
    queue.sort((a, b) => {
      if (a.status === b.status) {
        return new Date(a.createdDate).getTime() - new Date(b.createdDate).getTime();
      }
      return 0;
    });
  }

  /**
     * This function was designed to handle the capacity profile
     * A process might be executed based on rules
     * Ex: If system is working on high priority task, logic should be defined to still allow some low priority tasks to execute
     * if they have been seating for a while.
     *
     * Or decision could be made on the status of all servers.
     *
     * The capacity profile should contain those rules that will be executed in this function
     *
     * @param {Process} process
     */
  canServerExecuteProcessNow(process) {
    // since server is not running at full capactity, and nothing prevents this process from running...
    return true;
  }
}

module.exports = BasicCapacityProfile;
