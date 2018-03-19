var scClient = require('socketcluster-client');
var ClusterBrokerClient = require('./cluster-broker-client');
var uuid = require('uuid');

var DEFAULT_PORT = 7777;
var DEFAULT_MESSAGE_CACHE_DURATION = 10000;
var DEFAULT_RETRY_DELAY = 2000;
var DEFAULT_STATE_SERVER_CONNECT_TIMEOUT = 3000;
var DEFAULT_STATE_SERVER_ACK_TIMEOUT = 2000;

var DEFAULT_RECONNECT_RANDOMNESS = 1000;

module.exports.attach = function (broker, options) {
  var reconnectRandomness = options.stateServerReconnectRandomness || DEFAULT_RECONNECT_RANDOMNESS;
  var authKey = options.authKey || null;

  var clusterClient = new ClusterBrokerClient(broker, {authKey: authKey});
  if (!options.noErrorLogging) {
    clusterClient.on('error', (err) => {
      console.error(err);
    });
  }

  var messageCacheDuration = options.brokerMessageCacheDuration || DEFAULT_MESSAGE_CACHE_DURATION;
  var retryDelay = options.brokerRetryDelay || DEFAULT_RETRY_DELAY;

  var scStateSocketOptions = {
    hostname: options.stateServerHost, // Required option
    port: options.stateServerPort || DEFAULT_PORT,
    connectTimeout: options.stateServerConnectTimeout || DEFAULT_STATE_SERVER_CONNECT_TIMEOUT,
    ackTimeout: options.stateServerAckTimeout || DEFAULT_STATE_SERVER_ACK_TIMEOUT,
    autoReconnectOptions: {
      initialDelay: retryDelay,
      randomness: reconnectRandomness,
      multiplier: 1,
      maxDelay: retryDelay + reconnectRandomness
    },
    query: {
      authKey: authKey
    }
  };
  var stateSocket = scClient.connect(scStateSocketOptions);
  stateSocket.on('error', (err) => {
    clusterClient.emit('error', err);
  });

  var latestSnapshotTime = -1;

  var isNewSnapshot = (updatePacket) => {
    if (updatePacket.time > latestSnapshotTime) {
      latestSnapshotTime = updatePacket.time;
      return true;
    }
    return false;
  };

  var resetSnapshotTime = () => {
    latestSnapshotTime = -1;
  };

  var updateBrokerMapping = (data, respond) => {
    var updated = isNewSnapshot(data);
    if (updated) {
      clusterClient.setBrokers(data.sccBrokerInstances);
    }
    respond();
  };

  stateSocket.on('sccBrokerJoinCluster', updateBrokerMapping);
  stateSocket.on('sccBrokerLeaveCluster', updateBrokerMapping);

  var emitSCCWorkerJoinCluster = function () {
    stateSocket.emit('sccWorkerJoinCluster', stateSocketData, function (err, data) {
      if (err) {
        setTimeout(emitSCCWorkerJoinCluster, retryDelay);
        return;
      }
      resetSnapshotTime();
      clusterClient.setBrokers(data.sccBrokerInstances);
    });
  };
  stateSocket.on('connect', emitSCCWorkerJoinCluster);
};
