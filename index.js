var agClient = require('asyngular-client');
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
  if (!options.noErrorLogging) {
    (async () => {
      for await (let {error} of clusterClient.listener('error')) {
        console.error(error);
      }
    })();
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
      instanceType: 'agc-worker',
      version: packageVersion
    }
  };
  var stateSocket = agClient.create(scStateSocketOptions);
  (async () => {
    for await (let event of stateSocket.listener('error')) {
      clusterClient.emit('error', event);
    }
  })();

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

  var updateBrokerMapping = (req) => {
    var updated = isNewSnapshot(req.data);
    if (updated) {
      clusterClient.setBrokers(req.data.agcBrokerURIs);
    }
    req.end();
  };

  (async () => {
    for await (let req of stateSocket.procedure('agcBrokerJoinCluster')) {
      updateBrokerMapping(req);
    }
  })();
  (async () => {
    for await (let req of stateSocket.procedure('agcBrokerLeaveCluster')) {
      updateBrokerMapping(req);
    }
  })();

  var agcWorkerStateData = {
    instanceId: options.instanceId
  };

  agcWorkerStateData.instanceIp = options.instanceIp;
  agcWorkerStateData.instanceIpFamily = options.instanceIpFamily || 'IPv4';

  var emitAGCWorkerJoinCluster = async () => {
    let data;
    try {
      data = await stateSocket.invoke('agcWorkerJoinCluster');
    } catch (error) {
      stateSocket.emit('error', {error});
      setTimeout(emitAGCWorkerJoinCluster, retryDelay);
      return;
    }
    resetSnapshotTime();
    clusterClient.setBrokers(data.agcBrokerURIs);
  };

  (async () => {
    for await (let event of stateSocket.listener('connect')) {
      emitAGCWorkerJoinCluster();
    }
  })();

  var clusterMessageHandler = (channelName, packet) => {
    if ((packet.sender == null || packet.sender !== options.instanceId) && packet.messages && packet.messages.length) {
      packet.messages.forEach((data) => {
        broker.publish(channelName, data);
      });
    }
  };
  (async () => {
    for await (let {channel, packet} of clusterClient.listener('message')) {
      clusterMessageHandler(channel, packet);
    }
  })();

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
