const url = require('url');
const asyngularClient = require('asyngular-client');
const AsyncStreamEmitter = require('async-stream-emitter');
const Hasher = require('./hasher');

const trailingPortNumberRegex = /:[0-9]+$/;

function ClientPool(options) {
  AsyncStreamEmitter.call(this);

  options = options || {};
  this.hasher = new Hasher();
  this.clientCount = options.clientCount || 1;
  this.targetURI = options.targetURI;
  this.authKey = options.authKey;

  this.areClientListenersBound = false;

  let clientConnectOptions = this.breakDownURI(this.targetURI);
  clientConnectOptions.query = {
    authKey: this.authKey
  };

  this._handleClientError = (event) => {
    this.emit('error', event);
  };
  this._handleClientSubscribe = (channelName) => {
    let client = this;
    this.emit('subscribe', {
      targetURI: this.targetURI,
      poolIndex: client.poolIndex,
      channel: channelName
    });
  };
  this._handleClientSubscribeFail = (error, channelName) => {
    let client = this;
    this.emit('subscribeFail', {
      targetURI: this.targetURI,
      poolIndex: client.poolIndex,
      error,
      channel: channelName
    });
  };

  this.clients = [];

  for (let i = 0; i < this.clientCount; i++) {
    let connectOptions = Object.assign({}, clientConnectOptions);
    connectOptions.query.poolIndex = i;
    let client = asyngularClient.create(connectOptions);
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
  let parsedURI = url.parse(uri);
  let hostname = parsedURI.host.replace(trailingPortNumberRegex, '');
  let result = {
    hostname: hostname,
    port: parsedURI.port
  };
  if (parsedURI.protocol === 'wss:' || parsedURI.protocol === 'https:') {
    result.secure = true;
  }
  return result;
};

ClientPool.prototype.selectClient = function (key) {
  let targetIndex = this.hasher.hashToIndex(key, this.clients.length);
  return this.clients[targetIndex];
};

ClientPool.prototype.publish = function (channelName, data) {
  let targetClient = this.selectClient(channelName);
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
  let subscriptionList = [];
  this.clients.forEach((client) => {
    let clientSubList = client.subscriptions(includePending);
    clientSubList.forEach((subscription) => {
      subscriptionList.push(subscription);
    });
  });
  return subscriptionList;
};

ClientPool.prototype.subscribeAndWatch = function (channelName, handler) {
  let targetClient = this.selectClient(channelName);
  targetClient.subscribe(channelName);
  if (!targetClient.watchers(channelName).length) {
    targetClient.watch(channelName, (data) => {
      handler(data);
    });
  }
};

ClientPool.prototype.destroyChannel = function (channelName) {
  let targetClient = this.selectClient(channelName);
  return targetClient.destroyChannel(channelName);
};

ClientPool.prototype.destroy = function () {
  this.clients.forEach((client) => {
    client.destroy();
  });
};

module.exports = ClientPool;
