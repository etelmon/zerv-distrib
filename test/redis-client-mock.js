const _ = require('lodash');

module.exports = class RedisClientMock {
  constructor() {
    this.cache = {};
  }

  hset(list, key, value) {
    const queue = _.get(this.cache, list);
    if (!queue) {
      _.set(this.cache, list, [value]);
    } else {
      queue.push(value);
    }
  }


  hdel(list, key) {
    const queue = _.get(this.cache, list);
    if (queue) {
      _.remove(queue, (val) => JSON.parse(val).id === key);
    }
  }

  hvals(list) {
    return _.get(this.cache, list);
  }

  rpush(list, value) {
    this.hset(list, '', value);
  }
};
