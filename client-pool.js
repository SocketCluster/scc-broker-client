const url = require('url');
const socketClusterClient = require('socketcluster-client');
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

  let clientConnectOptions = this.breakDownURI(this.targetURI);
  clientConnectOptions.query = {
    authKey: this.authKey
  };

  this._handleClientError = (event) => {
    this.emit('error', event);
  };
  this._handleClientSubscribe = (client, channelName) => {
    this.emit('subscribe', {
      targetURI: this.targetURI,
      poolIndex: client.poolIndex,
      channel: channelName
    });
  };
  this._handleClientSubscribeFail = (client, error, channelName) => {
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
    let client = socketClusterClient.create(connectOptions);
    client.poolIndex = i;
    (async () => {
      for await (let event of client.listener('error')) {
        this._handleClientError(event);
      }
    })();
    this.clients.push(client);
  }

  this._bindClientListeners();
}

ClientPool.prototype = Object.create(AsyncStreamEmitter.prototype);

ClientPool.prototype._bindClientListeners = function () {
  this.clients.forEach((client) => {
    (async () => {
      for await (let event of client.listener('error')) {
        this._handleClientError(event);
      }
    })();
    (async () => {
      for await (let {channel} of client.listener('subscribe')) {
        this._handleClientSubscribe(client, channel);
      }
    })();
    (async () => {
      for await (let {error, channel} of client.listener('subscribeFail')) {
        this._handleClientSubscribeFail(client, error, channel);
      }
    })();
  });
};

ClientPool.prototype._unbindClientListeners = function () {
  this.clients.forEach((client) => {
    client.closeListener('error');
    client.closeListener('subscribe');
    client.closeListener('subscribeFail');
  });
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

ClientPool.prototype.invokePublish = async function (channelName, data) {
  let targetClient = this.selectClient(channelName);
  try {
    await targetClient.invokePublish(channelName, data);
  } catch (error) {
    this.emit('publishFail', {
      targetURI: this.targetURI,
      poolIndex: targetClient.poolIndex,
      channel: channelName,
      error
    });
    return;
  }
  this.emit('publish', {
    targetURI: this.targetURI,
    poolIndex: targetClient.poolIndex,
    channel: channelName,
    data
  });
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

ClientPool.prototype.isSubscribed = function (channelName, includePending) {
  let targetClient = this.selectClient(channelName);
  return targetClient.isSubscribed(channelName, includePending);
};

ClientPool.prototype.unsubscribe = function (channelName) {
  let targetClient = this.selectClient(channelName);
  return targetClient.unsubscribe(channelName);
};

ClientPool.prototype.subscribe = function (channelName) {
  let targetClient = this.selectClient(channelName);
  return targetClient.subscribe(channelName);
};

ClientPool.prototype.closeChannel = function (channelName) {
  let targetClient = this.selectClient(channelName);
  targetClient.closeChannel(channelName);
};

ClientPool.prototype.destroy = function () {
  this.clients.forEach((client) => {
    client.disconnect();
    client.subscriptions(true).forEach((channelName) => {
      client.unsubscribe(channelName);
      client.closeChannel(channelName);
    });
  });
  this._unbindClientListeners();
};

module.exports = ClientPool;
