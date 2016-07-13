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


  var getMapper = function (serverInstances) {
    return function (channelName) {
      var ch;
      var hash = channelName;

      for (var i = 0; i < channelName.length; i++) {
        ch = channelName.charCodeAt(i);
        hash = ((hash << 5) - hash) + ch;
        hash = hash & hash;
      }
      var targetIndex = Math.abs(hash) % serverInstances.length;
      return serverInstances[targetIndex];
    };
  };

  var sendClientState = function (stateName) {
    stateSocket.emit('clientSetState', {
      instanceState: stateName + ':' + JSON.stringify(serverInstances)
    });
  };

  var addNewSubMapping = function (data) {
    var updated = updateServerCluster(data);
    if (updated) {
      var mapper = getMapper(serverInstances);
      clusterClient.subMapperPush(mapper, serverInstances);
      sendClientState('updatedSubs');
    }
  };

  stateSocket.on('serverJoinCluster', addNewSubMapping);
  stateSocket.on('serverLeaveCluster', addNewSubMapping);

  stateSocket.on('clientStatesConverge', function (data) {
    if (data.state == 'updatedSubs:' + JSON.stringify(serverInstances)) {
      var mapper = getMapper(serverInstances);
      clusterClient.pubMapperPush(mapper, serverInstances);
      clusterClient.pubMapperShift(mapper);
      sendClientState('updatedPubs');
    } else if (data.state == 'updatedPubs:' + JSON.stringify(serverInstances)) {
      clusterClient.subMapperShift();
      sendClientState('active');
    }
  });

  stateSocket.emit('clientJoinCluster', stateSocketData, function (err, data) {
    updateServerCluster(data);
    var mapper = getMapper(serverInstances);
    clusterClient.subMapperPush(mapper, serverInstances);
    clusterClient.pubMapperPush(mapper, serverInstances);
    sendClientState('active');
  });
};
