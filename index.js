"use strict;"

var DEFAULT_MAX_SERVERS = 3;

var crypto = require('crypto');
var events = require('events');
var redis = require('redis');
var util = require('util');

function RedisHash(servers, max_servers) {
    this.max_servers = max_servers | DEFAULT_MAX_SERVERS;
    if (this.max_servers > servers.length)
        this.max_servers = servers.length;
    this.servers = []
    this.ready = false;
    var self = this;
    for (var x = 0; x < servers.length; x++) {
        var server = redis.createClient(servers[x].port, servers[x].host,
            servers[x].options);
        server.on('error', function(message) { self.emit('error', message); });
        server.on('ready', function () {
            if (self.ready == false) {
                self.ready = true;
                self.emit('ready');
            }});
        this.servers.push(server);
    }
    events.EventEmitter.call(this);
}

util.inherits(RedisHash, events.EventEmitter);

RedisHash.prototype.getServers = function (key) {
    var hash = crypto.createHash('md5').update('' + key).digest('hex');
    hash = parseInt(hash.substring(0, 8), 16);
    var servers = [];
    var remaining = [];
    for (var x = 0; x < this.servers.length; x++)
        remaining.push(x);
    for (var x = 0; x < this.max_servers; x++) {
        var server = remaining.splice(hash % remaining.length, 1)[0];
        server = this.servers[server];
        if (server.ready)
            servers.push(server);
    }
    return servers;
}

RedisHash.prototype.all = function (command, args, callback) {
    for (var x = 0; x < this.servers.length; x++)
        this.servers[x][command].apply(this.servers[x], args);
}

RedisHash.prototype.end = function () {
    for (var x = 0; x < this.servers.length; x++)
    {
        this.servers[x].end();
        // This is needed because the redis module clears stream event
        // handlers before calling end on the stream, and 'error' can still
        // be emitted.
        this.servers[x].stream.on('error', function () {});
    }
}

function RedisQueue(queue, servers, max_servers) {
    this.hash = new RedisHash(servers, max_servers);
    var self = this;
    this.hash.on('error', function (message) {
        self.emit('error', message);
        self.run_callback(message);
    });
    this.hash.on('ready', function () {
        self.emit('ready');
        self.flush();
    });
    this.queue = queue;
    this.data = queue + '-data';
    this.broken = queue + '-broken';
    this.pending = [];
    this.running = false;
    events.EventEmitter.call(this);
}

util.inherits(RedisQueue, events.EventEmitter);

RedisQueue.prototype.quit = function () {
    this.hash.all('quit');
}

RedisQueue.prototype.end = function () {
    this.hash.end();
}

RedisQueue.prototype.async = function (value, key, callback) {
    this.pending.push([this._async, value, key, callback]);
    this.flush();
}

RedisQueue.prototype._async = function (value, key, servers) {
    if (servers.length == 0) {
        this.run_callback('No servers available for key');
        return;
    }
    var server = servers.shift();
    var self = this;
    server.hexists(this.data, key, function (err, reply) {
        if (err)
            self._async(value, key, servers);
        else {
            server.hset(self.data, key, value, function (err, reply) {
                if (err)
                    self._async(value, key, servers);
            });
            if (!reply) {
                server.lpush(self.queue, key, function (err, reply) {
                    if (err)
                        self._async(value, key, servers);
                    self.run_callback();
                });
            }
        }
    });
}

RedisQueue.prototype.sync = function (value, key, callback) {
    this.pending.push([this._sync, value, key, callback]);
    this.flush();
}

RedisQueue.prototype._sync = function (value, key, servers) {
    if (servers.length == 0) {
        this.run_callback('No servers available for key');
        return;
    }
    var server = servers.shift();
    var self = this;
    server.hexists(this.data, key, function (err, reply) {
        if (err)
            self._sync(value, key, servers);
        else {
            var sub = redis.createClient(server.port, server.host,
                server.options);
            sub.on('error', function(message) {
                self.run_callback(message);
                sub.end();
            });
            sub.subscribe(key, function () {
                sub.on('message', function (channel, message) {
                    self.run_callback(undefined, message);
                    sub.end();
                });
                server.hset(self.data, key, value, function (err, reply) {
                    if (err)
                        self._sync(value, key, servers);
                });
                if (!reply) {
                    server.lpush(self.queue, key, function (err, reply) {
                        if (err) {
                            sub.end();
                            self._sync(value, key, servers);
                        }
                    });
                }
            });
        }
    });
}

RedisQueue.prototype.flush = function () {
    if (this.running || !this.hash.ready)
        return;
    var job = this.pending.shift();
    if (!job)
        return;
    key = job[2];
    if (key == null || key == undefined)
        key = Math.random() * 4294967296;
    this.running = true;
    this.callback = job[3];
    job[0].call(this, job[1], key, this.hash.getServers(key));
}

RedisQueue.prototype.run_callback = function () {
    if (this.callback)
        this.callback.apply(null, arguments);
    this.running = false;
    this.flush();
}

RedisQueue.prototype.work = function (callback) {
    for (var x = 0; x < this.hash.servers.length; x++)
        this._work(this.hash.servers[x], callback);
}

RedisQueue.prototype._work = function (server, callback) {
    var self = this;
    server.brpop(self.queue, 0, function (err, key) {
        if (err) {
            self.emit('error', err);
            return;
        }
        key = key[1];
        server.hget(self.data, key, function (err, data) {
            server.hdel(self.data, key);
            callback(key, data, function (result) {
                if (result != undefined)
                    server.publish(key, result);
                self._work(server, callback);
            });
        });
    });
}

exports.RedisHash = RedisHash;
exports.RedisQueue = RedisQueue;
