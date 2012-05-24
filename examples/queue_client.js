#!/usr/bin/env node

var redis_util = require('redis_util');

var queue = new redis_util.RedisQueue('test',
    [{port: 6379}, {port: 6380}, {port: 6381}]);
queue.on('ready', function () {
    queue.sync('payload', null, function (result) {
        console.log(result);
        queue.quit();
    });
});
