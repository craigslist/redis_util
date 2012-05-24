var redis_util = require('../');
var cluster = [{port: 6379}];

exports.sync_queue = function (test) {
    test.expect(3);
    var client = new redis_util.RedisQueue('sync_queue', cluster);
    var worker = new redis_util.RedisQueue('sync_queue', cluster);
    var on_error = function (message) {
        console.log(message);
        client.end();
        worker.end();
        test.done();
    }
    client.on('error', on_error);
    worker.on('error', on_error);
    worker.work(function (key, data, next) {
        test.equal(key, '12345');
        test.equal(data, 'payload');
        next('Result: ' + key + data);
    });
    client.sync('payload', 12345, function (error, result) {
        test.equal(result, 'Result: 12345payload');
        client.quit();
        worker.end();
        test.done();
    });
};

exports.async_queue = function (test) {
    test.expect(2);
    var client = new redis_util.RedisQueue('async_queue', cluster);
    var worker = new redis_util.RedisQueue('async_queue', cluster);
    var on_error = function (message) {
        console.log(message);
        client.end();
        worker.end();
        test.done();
    }
    client.on('error', on_error);
    worker.on('error', on_error);
    worker.work(function (key, data, next) {
        test.equal(key, '12345');
        test.equal(data, 'payload');
        client.quit();
        worker.end();
        test.done();
    });
    client.async('payload', 12345);
};
