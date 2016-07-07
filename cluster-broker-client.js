var url = require('url');
var scClient = require('socketcluster-client');

var ClusterBrokerClient = function () {
  this.subMappers = [];
  this.pubMappers = [];
};

ClusterBrokerClient.prototype.breakDownURI = function (uri) {
  var parsedURI = url.parse(uri);
  var result = {
    hostname: parsedURI.hostname,
    port: parsedURI.port
  };
  if (parsedURI.protocol == 'wss:' || parsedURI.protocol == 'https:') {
    result.secure = true;
  }
  return result;
};

ClusterBrokerClient.prototype._mapperPush = function (mapperList, mapper, targetURIs) {
  var targets = {};

  targetURIs.forEach((clientURI) => {
    var clientConnectOptions = this.breakDownURI(clientURI);
    var client = scClient.connect(clientConnectOptions);
    client.targetURI = clientURI;
    targets[clientURI] = client;
  });

  var mapperContext = {
    mapper: mapper,
    targets: targets
  };
  mapperList.push(mapperContext);

  return mapperContext;
};

ClusterBrokerClient.prototype.getAllSubscriptions = function () {
  var visitedClientsLookup = {};
  var channelsLookup = {};
  var subscriptions = [];
  this.subMappers.forEach((mapperContext) => {
    mapperContext.targets.forEach((client) => {
      if (!visitedClientsLookup[client.targetURI]) {
        visitedClientsLookup[client.targetURI] = true;
        var subs = client.getSubscriptions(true);
        subs.forEach((channelName) => {
          if (!channelsLookup[channelName]) {
            channelsLookup[channelName] = true;
            subscriptions.push(channelName);
          }
        });
      }
    });
  });
  return subscriptions;
};

ClusterBrokerClient.prototype.subMapperPush = function (mapper, targetURIs) {
  var mapperContext = this._mapperPush(this.subMappers, mapper, targetURIs);
  var activeChannels = this.getAllSubscriptions();

  activeChannels.forEach((channelName) => {
    this._subscribeWithMapperContext(mapperContext, channelName);
  });
};

ClusterBrokerClient.prototype.subMapperShift = function () {
  var oldMapperContext = this.subMappers.shift();
  var activeChannels = this.getAllSubscriptions();
  activeChannels.forEach((channelName) => {
    this._unsubscribeWithMapperContext(oldMapperContext, channelName);
  });
};

ClusterBrokerClient.prototype.pubMapperPush = function (mapper, targetURIs) {
  this._mapperPush(this.pubMappers, mapper, targetURIs);
};

ClusterBrokerClient.prototype.pubMapperShift = function () {
  this.pubMappers.shift();
};

ClusterBrokerClient.prototype._unsubscribeWithMapperContext = function (mapperContext, channelName) {
  var targetURI = mapperContext.mapper(channelName);
  var isLastRemainingMappingForClient = true;

  // If any other subscription mappers map to this client for this channel,
  // then don't unsubscribe.
  this.subMappers.forEach((subMapperContext) => {
    var subTargetURI = subMapperContext.mapper(channelName);
    if (targetURI == subTargetURI) {
      isLastRemainingMappingForClient = false;
      return;
    }
  });
  if (isLastRemainingMappingForClient) {
    var targetClient = mapperContext.targets[targetURI];
    targetClient.unsubscribe(channelName);
  }
};

ClusterBrokerClient.prototype._subscribeWithMapperContext = function (mapperContext, channelName) {
  var targetURI = mapperContext.mapper(channelName);
  var targetClient = mapperContext.targets[targetURI];
  targetClient.subscribe(channelName);
};

ClusterBrokerClient.prototype.subscribe = function (channelName) {
  this.subMappers.forEach((mapperContext) => {
    this._subscribeWithMapperContext(mapperContext, channelName);
  });
};

ClusterBrokerClient.prototype._publishWithMapperContext = function (mapperContext, channelName, data) {
  var targetURI = mapperContext.mapper(channelName);
  var targetClient = mapperContext.targets[targetURI];
  targetClient.publish(channelName, data);
};

ClusterBrokerClient.prototype.publish = function (channelName, data) {
  this.pubMappers.forEach((mapperContext) => {
    this._publishWithMapperContext(mapperContext, channelName, data);
  });
};

module.exports.ClusterBrokerClient = ClusterBrokerClient;
