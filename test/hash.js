var redis_util = require('../');
var cluster = [{port: 6379}, {port: 6379}, {port: 6379}, {port: 6379}];

exports.basic = function (test) {
    test.expect(1);
    var redis_hash = new redis_util.RedisHash(cluster);
    redis_hash.on('error', function (message) {
        console.log(message);
        redis_hash.end();
        test.done();
    });
    setTimeout(function () {
        test.equal(3, redis_hash.getServers('test').length);
        redis_hash.all('quit');
        test.done();
    }, 100);
};
