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
            const pos = _.findIndex(queue, (val) => JSON.parse(val).id === key);
            if (pos >= 0) {
                queue[pos] = value;
            } else {
                queue.push(value);
            }
        }
    }


    hdel(list, key) {
        const queue = _.get(this.cache, list);
        if (queue) {
            _.remove(queue, (val) => JSON.parse(val).id === key);
        }
    }

    hvals(list) {
        return _.get(this.cache, list, []);
    }

    rpush(list, value) {
        this.hset(list, '', value);
    }

    lrange(list) {
        return this.hvals(list);
    }
};
