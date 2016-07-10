var scClient = require('socketcluster-client');
var SCClusterBrokerClient = require('./cluster-broker-client').SCClusterBrokerClient;

module.exports.version = '1.0.0';

// TODO: When receiving messages from channels, check the UUID to make sure that
// each message is only processed once (particularly during a mapping transition).
// Also, make sure to only consider the servernInstances with the latest timestamp.

module.exports.attach = function (broker, options) {
  var clusterClient = new SCClusterBrokerClient();
  var lastestSnapshotTime = -1;
  var serverInstances = [];

  // TODO: Publish and subscribe to/from channels using clusterClient (relay channels)

  var updateServerCluster = function (updatePacket) {
    if (updatePacket.time > lastestSnapshotTime) {
      serverInstances = updatePacket.serverInstances;
      lastestSnapshotTime = updatePacket.time;
      return true;
    }
    return false;
  };

  var scStateSocketOptions = {
    hostname: '127.0.0.1', // TODO
    port: 7777 // TODO
  };
  var stateSocket = scClient.connect(scStateSocketOptions);
  var stateSocketData = {
    instanceId: broker.instanceId
  };

  stateSocket.on('serverJoinCluster', function (data) {
    // TODO: Subscribe to channels on updated server instances, keep old subscriptions
    // Then trigger a state change 'updatedSubs:xxx' for this client.
    var updated = updateServerCluster(data);
    if (updated) {
      var mapper = function (channelName) {
        // TODO: Return a URI based on hash of channelName
      };
      clusterClient.subMapperPush(mapper, serverInstances);
    }
  });
  stateSocket.on('serverLeaveCluster', function (data) {
    // TODO: Subscribe to channels on updated server instances, keep old subscriptions
    // Then trigger a state change 'updatedSubs:xxx' for this client.
    var updated = updateServerCluster(data);
  });

  stateSocket.on('clientStatesConverge', function (data) {
    if (data.state == 'updatedSubs:' + JSON.stringify(serverInstances)) {
      // This will run when all clients are subscribed to channels based on the new server instances mapping.
      // TODO: From now on, only publish based on the new mapping
      // then notify the cluster state server that this instance has updated its pubs.
    } else if (data.state == 'updatedPubs:' + JSON.stringify(serverInstances)) {
      // This will run when all clients are publishing based on the new server instances mapping.
      // TODO: Remove subscriptions which were based on the old mapping.
    }
  });

  stateSocket.emit('clientJoinCluster', stateSocketData, function (err, data) {
    var updated = updateServerCluster(data);
    // TODO: Subscribe to channels on server instances
  });
};
