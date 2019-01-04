var scClient = require('socketcluster-client');
var ClusterBrokerClient = require('./cluster-broker-client');
var packageVersion = require('./package.json').version;

var DEFAULT_PORT = 7777;
var DEFAULT_MESSAGE_CACHE_DURATION = 10000;
var DEFAULT_RETRY_DELAY = 2000;
var DEFAULT_STATE_SERVER_CONNECT_TIMEOUT = 3000;
var DEFAULT_STATE_SERVER_ACK_TIMEOUT = 2000;

var DEFAULT_RECONNECT_RANDOMNESS = 1000;

module.exports.attach = function (broker, options) {
  var reconnectRandomness = options.stateServerReconnectRandomness || DEFAULT_RECONNECT_RANDOMNESS;
  var authKey = options.authKey || null;

  var clusterClient = new ClusterBrokerClient(broker, {
    authKey: authKey,
    mappingEngine: options.mappingEngine,
    clientPoolSize: options.clientPoolSize,
  });
  if (options.noErrorLogging) {
    clusterClient.on('error', (err) => {});
  } else {
    clusterClient.on('error', (err) => {
      console.error(err);
    });
  }

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
      authKey,
      instancePort: options.instancePort,
      instanceType: 'scc-worker',
      version: packageVersion
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
      clusterClient.setBrokers(data.sccBrokerURIs);
    }
    respond();
  };

  stateSocket.on('sccBrokerJoinCluster', updateBrokerMapping);
  stateSocket.on('sccBrokerLeaveCluster', updateBrokerMapping);

  var sccWorkerStateData = {
    instanceId: options.instanceId
  };

  sccWorkerStateData.instanceIp = options.clusterInstanceIp;
  sccWorkerStateData.instanceIpFamily = options.clusterInstanceIpFamily || 'IPv4';

  var emitSCCWorkerJoinCluster = () => {
    stateSocket.emit('sccWorkerJoinCluster', sccWorkerStateData, (err, data) => {
      if (err) {
        setTimeout(emitSCCWorkerJoinCluster, retryDelay);
        return;
      }
      resetSnapshotTime();
      clusterClient.setBrokers(data.sccBrokerURIs);
    });
  };
  stateSocket.on('connect', emitSCCWorkerJoinCluster);

  var clusterMessageHandler = (channelName, packet) => {
    if ((packet.sender == null || packet.sender !== options.instanceId) && packet.messages && packet.messages.length) {
      packet.messages.forEach((data) => {
        broker.publish(channelName, data);
      });
    }
  };
  clusterClient.on('message', clusterMessageHandler);

  (async () => {
    for await (let {channel} of broker.listener('subscribe')) {
      clusterClient.subscribe(channelName);
    }
  })();

  (async () => {
    for await (let {channel} of broker.listener('unsubscribe')) {
      clusterClient.unsubscribe(channelName);
    }
  })();

  var publishOutboundBuffer = {};
  var publishTimeout = null;

  var flushPublishOutboundBuffer = () => {
    Object.keys(publishOutboundBuffer).forEach((channelName) => {
      var packet = {
        sender: options.instanceId || null,
        messages: publishOutboundBuffer[channelName],
      };
      clusterClient.publish(channelName, packet);
    });

    publishOutboundBuffer = {};
    publishTimeout = null;
  };

  (async () => {
    for await (let {channel, data} of broker.listener('publish')) {
      if (options.pubSubBatchDuration == null) {
        var packet = {
          sender: options.instanceId || null,
          messages: [data],
        };
        clusterClient.publish(channel, packet);
      } else {
        if (!publishOutboundBuffer[channel]) {
          publishOutboundBuffer[channel] = [];
        }
        publishOutboundBuffer[channel].push(data);

        if (!publishTimeout) {
          publishTimeout = setTimeout(flushPublishOutboundBuffer, options.pubSubBatchDuration);
        }
      }
    }
  })();

  return clusterClient;
};
