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

function RedisQueue(queue, servers, max_servers) {
    this.hash = new RedisHash(servers, max_servers);
    var self = this;
    this.hash.on('error', function (message) { self.emit('error', message); });
    this.hash.on('ready', function () { self.emit('ready'); });
    this.queue = queue;
    this.data = queue + '-data';
    this.broken = queue + '-broken';
    events.EventEmitter.call(this);
}

util.inherits(RedisQueue, events.EventEmitter);

RedisQueue.prototype.quit = function () {
    this.hash.all('quit');
}

RedisQueue.prototype.end = function () {
    this.hash.all('end');
}

RedisQueue.prototype.async = function (value, key, callback) {
    if (key == null || key == undefined)
        key = Math.random() * 4294967296;
    this._async(value, key, this.hash.getServers(key), callback);
}

RedisQueue.prototype._async = function (value, key, servers, callback) {
    if (servers.length == 0) {
        this.emit('error', new Error('No servers available for key'));
        return;
    }
    server = servers.splice(0, 1)[0];
    var self = this;
    server.hexists(this.data, key, function (err, reply) {
        if (err)
            self._async(value, key, servers, callback);
        else {
            server.hset(self.data, key, value, function (err, reply) {
                if (err)
                    self._async(value, key, servers, callback);
            });
            if (!reply) {
                server.lpush(self.queue, key, function (err, reply) {
                    if (err)
                        self._async(value, key, servers, callback);
                    else if (callback)
                        callback();
                });
            }
        }
    });
}

RedisQueue.prototype.sync = function (value, key, callback) {
    if (key == null || key == undefined)
        key = Math.random() * 4294967296;
    this._sync(value, key, this.hash.getServers(key), callback);
}

RedisQueue.prototype._sync = function (value, key, servers, callback) {
    if (servers.length == 0) {
        this.emit('error', new Error('No servers available for key'));
        return;
    }
    var server = servers.splice(0, 1)[0];
    var self = this;
    server.hexists(this.data, key, function (err, reply) {
        if (err)
            self._sync(value, key, servers, callback);
        else {
            var sub = redis.createClient(server.port, server.host,
                server.options);
            sub.subscribe(key, function () {
                sub.on('message', function (channel, message) {
                    callback(message);
                    sub.end();
                });
                server.hset(self.data, key, value, function (err, reply) {
                    if (err)
                        self._sync(value, key, servers, callback);
                });
                if (!reply) {
                    server.lpush(self.queue, key, function (err, reply) {
                        if (err) {
                            sub.end();
                            self._sync(value, key, servers, callback);
                        }
                    });
                }
            });
        }
    });
}

RedisQueue.prototype.work = function (callback) {
    for (var x = 0; x < this.hash.servers.length; x++)
        this._work(this.hash.servers[x], callback);
}

RedisQueue.prototype._work = function (server, callback) {
    var self = this;
    server.brpop(self.queue, 0, function (err, key) {
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
