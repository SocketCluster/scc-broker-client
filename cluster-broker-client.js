var ClientPool = require('./client-pool');
var EventEmitter = require('events').EventEmitter;
var SimpleMapper = require('./mappers/simple-mapper');
var SkeletonRendezvousMapper = require('./mappers/skeleton-rendezvous-mapper');

var ClusterBrokerClient = function (broker, options) {
  options = options || {};
  EventEmitter.call(this);
  this.broker = broker;
  this.agcBrokerClientPools = {};
  this.agcBrokerURIList = [];
  this.authKey = options.authKey || null;
  this.mappingEngine = options.mappingEngine || 'skeletonRendezvous';
  this.clientPoolSize = options.clientPoolSize || 1;

  if (this.mappingEngine === 'skeletonRendezvous') {
    this.mapper = new SkeletonRendezvousMapper(options.mappingEngineOptions);
  } else if (this.mappingEngine === 'simple') {
    this.mapper = new SimpleMapper(options.mappingEngineOptions);
  } else {
    if (typeof this.mappingEngine !== 'object') {
      throw new Error(`The specified mappingEngine '${this.mappingEngine}' is not a valid engine - It must be either 'simple', 'skeletonRendezvous' or a custom mappingEngine instance`);
    }
    this.mapper = this.mappingEngine;
  }
};

ClusterBrokerClient.prototype = Object.create(EventEmitter.prototype);

ClusterBrokerClient.prototype.errors = {
  NoMatchingSubscribeTargetError: function (channelName) {
    var err = new Error(`Could not find a matching subscribe target agc-broker for the ${channelName} channel - The agc-broker may be down`);
    err.name = 'NoMatchingSubscribeTargetError';
    return err;
  },
  NoMatchingUnsubscribeTargetError: function (channelName) {
    var err = new Error(`Could not find a matching unsubscribe target agc-broker for the ${channelName} channel - The agc-broker may be down`);
    err.name = 'NoMatchingUnsubscribeTargetError';
    return err;
  },
  NoMatchingPublishTargetError: function (channelName) {
    var err = new Error(`Could not find a matching publish target agc-broker for the ${channelName} channel - The agc-broker may be down`);
    err.name = 'NoMatchingPublishTargetError';
    return err;
  }
};

ClusterBrokerClient.prototype.mapChannelNameToBrokerURI = function (channelName) {
  return this.mapper.findSite(channelName);
};

ClusterBrokerClient.prototype.setBrokers = function (agcBrokerURIList) {
  this.agcBrokerURIList = agcBrokerURIList.concat();
  this.mapper.setSites(this.agcBrokerURIList);

  var brokerClientMap = {};
  var fullSubscriptionList = this.getAllSubscriptions();

  this.agcBrokerURIList.forEach((clientURI) => {
    var previousClientPool = this.agcBrokerClientPools[clientURI];
    if (previousClientPool) {
      previousClientPool.unbindClientListeners();
      previousClientPool.removeAllListeners();
    }
    var clientPool = new ClientPool({
      clientCount: this.clientPoolSize,
      targetURI: clientURI,
      authKey: this.authKey
    });
    clientPool.on('error', (err) => {
      this.emit('error', err);
    });
    clientPool.on('subscribe', (data) => {
      this.emit('subscribe', data);
    });
    clientPool.on('subscribeFail', (data) => {
      this.emit('subscribeFail', data);
    });
    clientPool.on('publish', (data) => {
      this.emit('publish', data);
    });
    clientPool.on('publishFail', (data) => {
      this.emit('publishFail', data);
    });
    clientPool.bindClientListeners();
    brokerClientMap[clientURI] = clientPool;
    this.agcBrokerClientPools[clientURI] = clientPool;
  });

  var unusedAGCBrokerURIList = Object.keys(this.agcBrokerClientPools).filter((clientURI) => {
    return !brokerClientMap[clientURI];
  });
  unusedAGCBrokerURIList.forEach((clientURI) => {
    var unusedClientPool = this.agcBrokerClientPools[clientURI];
    unusedClientPool.destroy();
    delete this.agcBrokerClientPools[clientURI];
  });

  var newSubscriptionsMap = {};
  fullSubscriptionList.forEach((channelName) => {
    var targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
    if (!newSubscriptionsMap[targetAGCBrokerURI]) {
      newSubscriptionsMap[targetAGCBrokerURI] = {};
    }
    if (!newSubscriptionsMap[targetAGCBrokerURI][channelName]) {
      newSubscriptionsMap[targetAGCBrokerURI][channelName] = true;
    }
  });

  Object.keys(this.agcBrokerClientPools).forEach((clientURI) => {
    var targetClientPool = this.agcBrokerClientPools[clientURI];
    var newChannelLookup = newSubscriptionsMap[clientURI] || {};

    var existingChannelList = targetClientPool.subscriptions(true);
    existingChannelList.forEach((channelName) => {
      if (!newChannelLookup[channelName]) {
        targetClientPool.destroyChannel(channelName);
      }
    });

    var newChannelList = Object.keys(newChannelLookup);
    newChannelList.forEach((channelName) => {
      this._subscribeClientPoolToChannelAndWatch(targetClientPool, channelName);
    });
  });
};

ClusterBrokerClient.prototype.getAllSubscriptions = function () {
  var channelLookup = {};

  Object.keys(this.agcBrokerClientPools).forEach((clientURI) => {
    var clientPool = this.agcBrokerClientPools[clientURI];
    var subs = clientPool.subscriptions(true);
    subs.forEach((channelName) => {
      if (!channelLookup[channelName]) {
        channelLookup[channelName] = true;
      }
    });
  });
  var localBrokerSubscriptions = this.broker.subscriptions();
  localBrokerSubscriptions.forEach((channelName) => {
    channelLookup[channelName] = true;
  });
  return Object.keys(channelLookup);
};

ClusterBrokerClient.prototype._handleChannelMessage = function (channelName, packet) {
  this.emit('message', channelName, packet);
};

ClusterBrokerClient.prototype._subscribeClientPoolToChannelAndWatch = function (clientPool, channelName) {
  clientPool.subscribeAndWatch(channelName, (data) => {
    this._handleChannelMessage(channelName, data);
  });
};

ClusterBrokerClient.prototype.subscribe = function (channelName) {
  var targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    this._subscribeClientPoolToChannelAndWatch(targetAGCBrokerClientPool, channelName);
  } else {
    var err = this.errors.NoMatchingSubscribeTargetError(channelName);
    this.emit('error', err);
  }
};

ClusterBrokerClient.prototype.unsubscribe = function (channelName) {
  var targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    targetAGCBrokerClientPool.destroyChannel(channelName);
  } else {
    var err = this.errors.NoMatchingUnsubscribeTargetError(channelName);
    this.emit('error', err);
  }
};

ClusterBrokerClient.prototype.publish = function (channelName, data) {
  var targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    targetAGCBrokerClientPool.publish(channelName, data);
  } else {
    var err = this.errors.NoMatchingPublishTargetError(channelName);
    this.emit('error', err);
  }
};

module.exports = ClusterBrokerClient;
