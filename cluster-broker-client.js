const ClientPool = require('./client-pool');
const AsyncStreamEmitter = require('async-stream-emitter');
const SimpleMapper = require('./mappers/simple-mapper');
const SkeletonRendezvousMapper = require('./mappers/skeleton-rendezvous-mapper');

function ClusterBrokerClient(broker, options) {
  AsyncStreamEmitter.call(this);

  options = options || {};
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
}

ClusterBrokerClient.prototype = Object.create(AsyncStreamEmitter.prototype);

ClusterBrokerClient.prototype.errors = {
  NoMatchingSubscribeTargetError: function (channelName) {
    let err = new Error(`Could not find a matching subscribe target agc-broker for the ${channelName} channel - The agc-broker may be down`);
    err.name = 'NoMatchingSubscribeTargetError';
    return err;
  },
  NoMatchingUnsubscribeTargetError: function (channelName) {
    let err = new Error(`Could not find a matching unsubscribe target agc-broker for the ${channelName} channel - The agc-broker may be down`);
    err.name = 'NoMatchingUnsubscribeTargetError';
    return err;
  },
  NoMatchingPublishTargetError: function (channelName) {
    let err = new Error(`Could not find a matching publish target agc-broker for the ${channelName} channel - The agc-broker may be down`);
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

  let brokerClientMap = {};
  let fullSubscriptionList = this.getAllSubscriptions();

  this.agcBrokerURIList.forEach((clientURI) => {
    let previousClientPool = this.agcBrokerClientPools[clientURI];
    if (previousClientPool) {
      previousClientPool.unbindClientListeners();
      previousClientPool.closeAllListeners();
    }
    let clientPool = new ClientPool({
      clientCount: this.clientPoolSize,
      targetURI: clientURI,
      authKey: this.authKey
    });
    (async () => {
      for await (let event of clientPool.listener('error')) {
        this.emit('error', event);
      }
    })();
    (async () => {
      for await (let event of clientPool.listener('subscribe')) {
        this.emit('subscribe', event);
      }
    })();
    (async () => {
      for await (let event of clientPool.listener('subscribeFail')) {
        this.emit('subscribeFail', event);
      }
    })();
    (async () => {
      for await (let event of clientPool.listener('publish')) {
        this.emit('publish', event);
      }
    })();
    (async () => {
      for await (let event of clientPool.listener('publishFail')) {
        this.emit('publishFail', event);
      }
    })();
    clientPool.bindClientListeners();
    brokerClientMap[clientURI] = clientPool;
    this.agcBrokerClientPools[clientURI] = clientPool;
  });

  let unusedAGCBrokerURIList = Object.keys(this.agcBrokerClientPools).filter((clientURI) => {
    return !brokerClientMap[clientURI];
  });
  unusedAGCBrokerURIList.forEach((clientURI) => {
    let unusedClientPool = this.agcBrokerClientPools[clientURI];
    unusedClientPool.destroy();
    delete this.agcBrokerClientPools[clientURI];
  });

  let newSubscriptionsMap = {};
  fullSubscriptionList.forEach((channelName) => {
    let targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
    if (!newSubscriptionsMap[targetAGCBrokerURI]) {
      newSubscriptionsMap[targetAGCBrokerURI] = {};
    }
    if (!newSubscriptionsMap[targetAGCBrokerURI][channelName]) {
      newSubscriptionsMap[targetAGCBrokerURI][channelName] = true;
    }
  });

  Object.keys(this.agcBrokerClientPools).forEach((clientURI) => {
    let targetClientPool = this.agcBrokerClientPools[clientURI];
    let newChannelLookup = newSubscriptionsMap[clientURI] || {};

    let existingChannelList = targetClientPool.subscriptions(true);
    existingChannelList.forEach((channelName) => {
      if (!newChannelLookup[channelName]) {
        targetClientPool.closeChannel(channelName);
      }
    });

    let newChannelList = Object.keys(newChannelLookup);
    newChannelList.forEach((channelName) => {
      this._subscribeClientPoolToChannelAndWatch(targetClientPool, channelName);
    });
  });
};

ClusterBrokerClient.prototype.getAllSubscriptions = function () {
  let channelLookup = {};

  Object.keys(this.agcBrokerClientPools).forEach((clientURI) => {
    let clientPool = this.agcBrokerClientPools[clientURI];
    let subs = clientPool.subscriptions(true);
    subs.forEach((channelName) => {
      if (!channelLookup[channelName]) {
        channelLookup[channelName] = true;
      }
    });
  });
  let localBrokerSubscriptions = this.broker.subscriptions();
  localBrokerSubscriptions.forEach((channelName) => {
    channelLookup[channelName] = true;
  });
  return Object.keys(channelLookup);
};

ClusterBrokerClient.prototype._handleChannelMessage = function (channelName, packet) {
  this.emit('message', {
    channel: channelName,
    packet
  });
};

ClusterBrokerClient.prototype._subscribeClientPoolToChannelAndWatch = function (clientPool, channelName) {
  clientPool.closeChannel(channelName);
  let channel = clientPool.subscribe(channelName);

  (async () => {
    for await (let data of channel) {
      this._handleChannelMessage(channelName, data);
    }
  })();
};

ClusterBrokerClient.prototype.subscribe = function (channelName) {
  let targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    this._subscribeClientPoolToChannelAndWatch(targetAGCBrokerClientPool, channelName);
  } else {
    let error = this.errors.NoMatchingSubscribeTargetError(channelName);
    this.emit('error', {error});
  }
};

ClusterBrokerClient.prototype.unsubscribe = function (channelName) {
  let targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    targetAGCBrokerClientPool.closeChannel(channelName);
  } else {
    let error = this.errors.NoMatchingUnsubscribeTargetError(channelName);
    this.emit('error', {error});
  }
};

ClusterBrokerClient.prototype.publish = function (channelName, data) {
  let targetAGCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetAGCBrokerClientPool = this.agcBrokerClientPools[targetAGCBrokerURI];
  if (targetAGCBrokerClientPool) {
    targetAGCBrokerClientPool.publish(channelName, data);
  } else {
    let error = this.errors.NoMatchingPublishTargetError(channelName);
    this.emit('error', {error});
  }
};

module.exports = ClusterBrokerClient;
