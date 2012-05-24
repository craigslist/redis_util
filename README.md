Redis Utils
===========

A collection of utilities for node-redis, such as a hashing frontend and
queue interface.

    18:35 < godsflaw> it's the node bits one needs for using redis as a job processing queue


Installation
============

To install with [npm](http://github.com/isaacs/npm):
 
    npm install redis_util

Examples
========

Insert a key/value pair into the appropriate server after hasing a key.

```javascript
    var key = 'test';
    var redis_hash = new RedisHash([{host: '10.0.0.1'}, {host: '10.0.0.2'}]);
    redis_hash.getServers(key)[0].set(key, 'some data for key');
```

Insert data into a queue and wait for a response:

```javascript
var cluster = [{host: '10.0.0.1'}, {host: '10.0.0.2'}];
var queue = new RedisQueue('test', cluster);
queue.on('ready', function () {
    queue.sync('payload', null, function (result) {
        console.log(result);
        queue.quit();
    });
});
```

Run a worker and return uppercase version of payload.

```javascript
var worker = new RedisQueue('test', cluster);
worker.work(function (key, data, next) { next(data.toUpperCase()); });
```
