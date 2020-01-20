const ClientPool = require('./client-pool');
const AsyncStreamEmitter = require('async-stream-emitter');
const SimpleMapper = require('./mappers/simple-mapper');
const SkeletonRendezvousMapper = require('./mappers/skeleton-rendezvous-mapper');

function ClusterBrokerClient(broker, options) {
  AsyncStreamEmitter.call(this);

  options = options || {};
  this.broker = broker;
  this.sccBrokerClientPools = {};
  this.sccBrokerURIList = [];
  this.authKey = options.authKey || null;
  this.mappingEngine = options.mappingEngine || 'skeletonRendezvous';
  this.clientPoolSize = options.clientPoolSize || 1;
  this.isReady = false;

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
    let err = new Error(`Could not find a matching subscribe target scc-broker for the ${channelName} channel - The scc-broker may be down`);
    err.name = 'NoMatchingSubscribeTargetError';
    return err;
  },
  NoMatchingUnsubscribeTargetError: function (channelName) {
    let err = new Error(`Could not find a matching unsubscribe target scc-broker for the ${channelName} channel - The scc-broker may be down`);
    err.name = 'NoMatchingUnsubscribeTargetError';
    return err;
  },
  NoMatchingPublishTargetError: function (channelName) {
    let err = new Error(`Could not find a matching publish target scc-broker for the ${channelName} channel - The scc-broker may be down`);
    err.name = 'NoMatchingPublishTargetError';
    return err;
  }
};

ClusterBrokerClient.prototype.mapChannelNameToBrokerURI = function (channelName) {
  return this.mapper.findSite(channelName);
};

ClusterBrokerClient.prototype.setBrokers = function (sccBrokerURIList) {
  this.sccBrokerURIList = sccBrokerURIList.concat();
  this.mapper.setSites(this.sccBrokerURIList);

  let brokerClientMap = {};
  let fullSubscriptionList = this.getAllSubscriptions();

  this.sccBrokerURIList.forEach((clientURI) => {
    if (this.sccBrokerClientPools[clientURI]) {
      brokerClientMap[clientURI] = this.sccBrokerClientPools[clientURI];
      return;
    }

    let clientPool = new ClientPool({
      clientCount: this.clientPoolSize,
      targetURI: clientURI,
      authKey: this.authKey
    });
    brokerClientMap[clientURI] = clientPool;
    this.sccBrokerClientPools[clientURI] = clientPool;

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
  });

  let unusedsccBrokerURIList = Object.keys(this.sccBrokerClientPools).filter((clientURI) => {
    return !brokerClientMap[clientURI];
  });
  unusedsccBrokerURIList.forEach((clientURI) => {
    let unusedClientPool = this.sccBrokerClientPools[clientURI];
    unusedClientPool.destroy();
    delete this.sccBrokerClientPools[clientURI];
  });

  let newSubscriptionsMap = {};
  fullSubscriptionList.forEach((channelName) => {
    let targetsccBrokerURI = this.mapChannelNameToBrokerURI(channelName);
    if (!newSubscriptionsMap[targetsccBrokerURI]) {
      newSubscriptionsMap[targetsccBrokerURI] = {};
    }
    if (!newSubscriptionsMap[targetsccBrokerURI][channelName]) {
      newSubscriptionsMap[targetsccBrokerURI][channelName] = true;
    }
  });

  Object.keys(this.sccBrokerClientPools).forEach((clientURI) => {
    let targetClientPool = this.sccBrokerClientPools[clientURI];
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

  this.emit('updateBrokers', {brokerURIs: sccBrokerURIList});
  this.isReady = true;
};

ClusterBrokerClient.prototype.getAllSubscriptions = function () {
  let channelLookup = {};

  Object.keys(this.sccBrokerClientPools).forEach((clientURI) => {
    let clientPool = this.sccBrokerClientPools[clientURI];
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
  if (clientPool.isSubscribed(channelName, true)) {
    return;
  }
  let channel = clientPool.subscribe(channelName);
  (async () => {
    for await (let data of channel) {
      this._handleChannelMessage(channelName, data);
    }
  })();
};

ClusterBrokerClient.prototype.subscribe = function (channelName) {
  let targetsccBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetsccBrokerClientPool = this.sccBrokerClientPools[targetsccBrokerURI];
  if (targetsccBrokerClientPool) {
    this._subscribeClientPoolToChannelAndWatch(targetsccBrokerClientPool, channelName);
  } else {
    let error = this.errors.NoMatchingSubscribeTargetError(channelName);
    this.emit('error', {error});
  }
};

ClusterBrokerClient.prototype.unsubscribe = function (channelName) {
  let targetsccBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetsccBrokerClientPool = this.sccBrokerClientPools[targetsccBrokerURI];
  if (targetsccBrokerClientPool) {
    targetsccBrokerClientPool.unsubscribe(channelName);
    targetsccBrokerClientPool.closeChannel(channelName);
  } else {
    let error = this.errors.NoMatchingUnsubscribeTargetError(channelName);
    this.emit('error', {error});
  }
};

ClusterBrokerClient.prototype.invokePublish = function (channelName, data) {
  let targetsccBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  let targetsccBrokerClientPool = this.sccBrokerClientPools[targetsccBrokerURI];
  if (targetsccBrokerClientPool) {
    targetsccBrokerClientPool.invokePublish(channelName, data);
  } else {
    let error = this.errors.NoMatchingPublishTargetError(channelName);
    this.emit('error', {error});
  }
};

module.exports = ClusterBrokerClient;
