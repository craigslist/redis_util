#!/usr/bin/env node

var redis_util = require('redis_util');

var worker = new redis_util.RedisQueue('test',
    [{port: 6379}, {port: 6380}, {port: 6381}]);
worker.work(function (key, data, next) { next(data.toUpperCase()); });
