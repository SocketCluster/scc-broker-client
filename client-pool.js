var url = require('url');
var agClient = require('asyngular-client');
var AsyncStreamEmitter = require('async-stream-emitter');
var Hasher = require('./hasher');

var trailingPortNumberRegex = /:[0-9]+$/;

function ClientPool(options) {
  AsyncStreamEmitter.call(this);
  var self = this;

  options = options || {};
  this.hasher = new Hasher();
  this.clientCount = options.clientCount || 1;
  this.targetURI = options.targetURI;
  this.authKey = options.authKey;

  this.areClientListenersBound = false;

  var clientConnectOptions = this.breakDownURI(this.targetURI);
  clientConnectOptions.query = {
    authKey: this.authKey
  };

  this._handleClientError = function (event) {
    self.emit('error', event);
  };
  this._handleClientSubscribe = function (channelName) {
    var client = this;
    self.emit('subscribe', {
      targetURI: self.targetURI,
      poolIndex: client.poolIndex,
      channel: channelName
    });
  };
  this._handleClientSubscribeFail = function (error, channelName) {
    var client = this;
    self.emit('subscribeFail', {
      targetURI: self.targetURI,
      poolIndex: client.poolIndex,
      error,
      channel: channelName
    });
  };

  this.clients = [];

  for (var i = 0; i < this.clientCount; i++) {
    var connectOptions = Object.assign({}, clientConnectOptions);
    connectOptions.query.poolIndex = i;
    var client = agClient.create(connectOptions);
    client.poolIndex = i;
    (async () => {
      for await (let event of client.listener('error')) {
        this._handleClientError(event);
      }
    })();
    this.clients.push(client);
  }
}

ClientPool.prototype = Object.create(AsyncStreamEmitter.prototype);

ClientPool.prototype.bindClientListeners = function () {
  this.unbindClientListeners();
  this.clients.forEach((client) => {
    (async () => {
      for await (let event of client.listener('error')) {
        this._handleClientError(event);
      }
    })();
    (async () => {
      for await (let {channel} of client.listener('subscribe')) {
        this._handleClientSubscribe(channel);
      }
    })();
    (async () => {
      for await (let {error, channel} of client.listener('subscribeFail')) {
        this._handleClientSubscribeFail(error, channel);
      }
    })();
  });
  this.areClientListenersBound = true;
};

ClientPool.prototype.unbindClientListeners = function () {
  this.clients.forEach((client) => {
    client.closeListener('error');
    client.closeListener('subscribe');
    client.closeListener('subscribeFail');
  });
  this.areClientListenersBound = false;
};

ClientPool.prototype.breakDownURI = function (uri) {
  var parsedURI = url.parse(uri);
  var hostname = parsedURI.host.replace(trailingPortNumberRegex, '');
  var result = {
    hostname: hostname,
    port: parsedURI.port
  };
  if (parsedURI.protocol === 'wss:' || parsedURI.protocol === 'https:') {
    result.secure = true;
  }
  return result;
};

ClientPool.prototype.selectClient = function (key) {
  var targetIndex = this.hasher.hashToIndex(key, this.clients.length);
  return this.clients[targetIndex];
};

ClientPool.prototype.publish = function (channelName, data) {
  var targetClient = this.selectClient(channelName);
  if (this.areClientListenersBound) {
    return targetClient.publish(channelName, data, (err) => {
      if (!this.areClientListenersBound) {
        return;
      }
      if (err) {
        this.emit('publishFail', {
          targetURI: this.targetURI,
          poolIndex: targetClient.poolIndex,
          channel: channelName,
          data
        });
        return;
      }
      this.emit('publish', {
        targetURI: this.targetURI,
        poolIndex: targetClient.poolIndex,
        channel: channelName,
        data
      });
    });
  }
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
