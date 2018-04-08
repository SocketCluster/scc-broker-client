var url = require('url');
var scClient = require('socketcluster-client');
var EventEmitter = require('events').EventEmitter;
var SimpleMapper = require('./mappers/simple-mapper');
var SkeletonRendezvousMapper = require('./mappers/skeleton-rendezvous-mapper');

var trailingPortNumberRegex = /:[0-9]+$/;

var ClusterBrokerClient = function (broker, options) {
  options = options || {};
  EventEmitter.call(this);
  this.broker = broker;
  this.sccBrokerClients = {};
  this.sccBrokerURIList = [];
  this.authKey = options.authKey || null;
  this.mappingEngine = options.mappingEngine || 'skeletonRendezvous';

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

  this._handleClientError = (err) => {
    this.emit('error', err);
  };
};

ClusterBrokerClient.prototype = Object.create(EventEmitter.prototype);

ClusterBrokerClient.prototype.errors = {
  NoMatchingSubscribeTargetError: function (channelName) {
    var err = new Error(`Could not find a matching subscribe target scc-broker for the ${channelName} channel - The scc-broker may be down.`);
    err.name = 'NoMatchingSubscribeTargetError';
    return err;
  },
  NoMatchingUnsubscribeTargetError: function (channelName) {
    var err = new Error(`Could not find a matching unsubscribe target scc-broker for the ${channelName} channel - The scc-broker may be down.`);
    err.name = 'NoMatchingUnsubscribeTargetError';
    return err;
  },
  NoMatchingPublishTargetError: function (channelName) {
    var err = new Error(`Could not find a matching publish target scc-broker for the ${channelName} channel - The scc-broker may be down.`);
    err.name = 'NoMatchingPublishTargetError';
    return err;
  }
};

ClusterBrokerClient.prototype.mapChannelNameToBrokerURI = function (channelName) {
  return this.mapper.findSite(channelName);
};

ClusterBrokerClient.prototype.breakDownURI = function (uri) {
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

ClusterBrokerClient.prototype.setBrokers = function (sccBrokerURIList) {
  this.sccBrokerURIList = sccBrokerURIList.concat();
  this.mapper.setSites(this.sccBrokerURIList);

  var brokerClientMap = {};
  var fullSubscriptionList = this.getAllSubscriptions();

  this.sccBrokerURIList.forEach((clientURI) => {
    var clientConnectOptions = this.breakDownURI(clientURI);
    clientConnectOptions.query = {
      authKey: this.authKey
    };
    // Will reuse client if it already exists for the URI.
    var client = scClient.create(clientConnectOptions);
    client.removeListener('error', this._handleClientError);
    client.on('error', this._handleClientError);
    client.targetURI = clientURI;
    brokerClientMap[clientURI] = client;
    this.sccBrokerClients[clientURI] = client;
  });

  var unusedSCCBrokerURIList = Object.keys(this.sccBrokerClients).filter((clientURI) => {
    return !brokerClientMap[clientURI];
  });
  unusedSCCBrokerURIList.forEach((clientURI) => {
    var unusedClient = this.sccBrokerClients[clientURI];
    unusedClient.destroy();
    delete this.sccBrokerClients[clientURI];
  });

  var newSubscriptionsMap = {};
  fullSubscriptionList.forEach((channelName) => {
    var targetSCCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
    if (!newSubscriptionsMap[targetSCCBrokerURI]) {
      newSubscriptionsMap[targetSCCBrokerURI] = {};
    }
    if (!newSubscriptionsMap[targetSCCBrokerURI][channelName]) {
      newSubscriptionsMap[targetSCCBrokerURI][channelName] = true;
    }
  });

  Object.keys(this.sccBrokerClients).forEach((clientURI) => {
    var targetClient = this.sccBrokerClients[clientURI];
    var newChannelLookup = newSubscriptionsMap[clientURI] || {};

    var existingChannelList = targetClient.subscriptions(true);
    existingChannelList.forEach((channelName) => {
      if (!newChannelLookup[channelName]) {
        targetClient.destroyChannel(channelName);
      }
    });

    var newChannelList = Object.keys(newChannelLookup);
    newChannelList.forEach((channelName) => {
      this._subscribeClientToChannelAndWatch(targetClient, channelName);
    });
  });
};

ClusterBrokerClient.prototype._getAllUpstreamBrokerSubscriptions = function () {
  var channelMap = {};
  var workerChannelMaps = Object.keys(this.broker.subscriptions);
  workerChannelMaps.forEach((index) => {
    var workerChannels = Object.keys(this.broker.subscriptions[index]);
    workerChannels.forEach((channelName) => {
      channelMap[channelName] = true;
    });
  });
  return Object.keys(channelMap);
};

ClusterBrokerClient.prototype.getAllSubscriptions = function () {
  var visitedClientLookup = {};
  var channelLookup = {};

  Object.keys(this.sccBrokerClients).forEach((clientURI) => {
    var client = this.sccBrokerClients[clientURI];
    if (!visitedClientLookup[clientURI]) {
      visitedClientLookup[clientURI] = true;
      var subs = client.subscriptions(true);
      subs.forEach((channelName) => {
        if (!channelLookup[channelName]) {
          channelLookup[channelName] = true;
        }
      });
    }
  });
  var localBrokerSubscriptions = this._getAllUpstreamBrokerSubscriptions();
  localBrokerSubscriptions.forEach((channelName) => {
    channelLookup[channelName] = true;
  });
  return Object.keys(channelLookup);
};

ClusterBrokerClient.prototype._handleChannelMessage = function (channelName, packet) {
  this.emit('message', channelName, packet);
};

ClusterBrokerClient.prototype._subscribeClientToChannelAndWatch = function (client, channelName) {
  client.subscribe(channelName);
  if (!client.watchers(channelName).length) {
    client.watch(channelName, (data) => {
      this._handleChannelMessage(channelName, data);
    });
  }
};

ClusterBrokerClient.prototype.subscribe = function (channelName) {
  var targetSCCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetSCCBrokerClient = this.sccBrokerClients[targetSCCBrokerURI];
  if (targetSCCBrokerClient) {
    this._subscribeClientToChannelAndWatch(targetSCCBrokerClient, channelName);
  } else {
    var err = this.errors.NoMatchingSubscribeTargetError(channelName);
    this.emit('error', err);
  }
};

ClusterBrokerClient.prototype.unsubscribe = function (channelName) {
  var targetSCCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetSCCBrokerClient = this.sccBrokerClients[targetSCCBrokerURI];
  if (targetSCCBrokerClient) {
    targetSCCBrokerClient.destroyChannel(channelName);
  } else {
    var err = this.errors.NoMatchingUnsubscribeTargetError(channelName);
    this.emit('error', err);
  }
};

ClusterBrokerClient.prototype.publish = function (channelName, data) {
  var targetSCCBrokerURI = this.mapChannelNameToBrokerURI(channelName);
  var targetSCCBrokerClient = this.sccBrokerClients[targetSCCBrokerURI];
  if (targetSCCBrokerClient) {
    targetSCCBrokerClient.publish(channelName, data);
  } else {
    var err = this.errors.NoMatchingPublishTargetError(channelName);
    this.emit('error', err);
  }
};

module.exports = ClusterBrokerClient;
