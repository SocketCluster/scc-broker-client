var url = require('url');
var scClient = require('socketcluster-client');
var EventEmitter = require('events').EventEmitter;
var Hasher = require('./hasher');

var trailingPortNumberRegex = /:[0-9]+$/;

function ClientPool(options) {
  EventEmitter.call(this);

  options = options || {};
  this.hasher = new Hasher();
  this.clientCount = options.clientCount || 1;
  this.targetURI = options.targetURI;
  this.authKey = options.authKey;

  var clientConnectOptions = this.breakDownURI(this.targetURI);
  clientConnectOptions.query = {
    authKey: this.authKey
  };

  this._handleClientError = (err) => {
    this.emit('error', err);
  };

  this.clients = [];
  for (var i = 0; i < this.clientCount; i++) {
    var client = scClient.create(clientConnectOptions);
    client.removeListener('error', this._handleClientError);
    client.on('error', this._handleClientError);
    this.clients.push(client);
  }
}

ClientPool.prototype = Object.create(EventEmitter.prototype);

ClientPool.prototype.breakDownURI = function (uri) {
  var parsedURI = url.parse(uri);
  var hostname = parsedURI.host.replace(trailingPortNumberRegex, '');
  var result = {
    hostname: hostname,
    port: parsedURI.port
  };
  if (parsedURI.protocol == 'wss:' || parsedURI.protocol == 'https:') {
    result.secure = true;
  }
  return result;
};

ClientPool.prototype.selectClient = function (key) {
  var targetIndex = this.hasher.hash(key, this.clients.length);
  return this.clients[targetIndex];
};

ClientPool.prototype.publish = function (channelName, data) {
  var targetClient = this.selectClient(channelName);
  return targetClient.publish(channelName, data);
};

ClientPool.prototype.subscriptions = function (includePending) {
  var subscriptionList = [];
  this.clients.forEach((client) => {
    var clientSubList = client.subscriptions(includePending);
    clientSubList.forEach((subscription) => {
      subscriptionList.push(subscription);
    });
  });
  return subscriptionList;
};

ClientPool.prototype.subscribeAndWatch = function (channelName, handler) {
  var targetClient = this.selectClient(channelName);
  targetClient.subscribe(channelName);
  if (!targetClient.watchers(channelName).length) {
    targetClient.watch(channelName, (data) => {
      handler(data);
    });
  }
};

ClientPool.prototype.destroyChannel = function (channelName) {
  var targetClient = this.selectClient(channelName);
  return targetClient.destroyChannel(channelName);
};

ClientPool.prototype.destroy = function () {
  this.clients.forEach((client) => {
    client.destroy();
  });
};

module.exports = ClientPool;
