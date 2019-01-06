const asyngularClient = require('asyngular-client');
const ClusterBrokerClient = require('./cluster-broker-client');
const packageVersion = require('./package.json').version;

const DEFAULT_PORT = 7777;
const DEFAULT_RETRY_DELAY = 2000;
const DEFAULT_STATE_SERVER_CONNECT_TIMEOUT = 3000;
const DEFAULT_STATE_SERVER_ACK_TIMEOUT = 2000;

const DEFAULT_RECONNECT_RANDOMNESS = 1000;

module.exports.attach = function (broker, options) {
  let reconnectRandomness = options.stateServerReconnectRandomness || DEFAULT_RECONNECT_RANDOMNESS;
  let authKey = options.authKey || null;

  let clusterClient = new ClusterBrokerClient(broker, {
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

  let retryDelay = options.brokerRetryDelay || DEFAULT_RETRY_DELAY;

  let scStateSocketOptions = {
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
  let stateSocket = asyngularClient.create(scStateSocketOptions);
  (async () => {
    for await (let event of stateSocket.listener('error')) {
      clusterClient.emit('error', event);
    }
  })();

  let latestSnapshotTime = -1;

  let isNewSnapshot = (updatePacket) => {
    if (updatePacket.time > latestSnapshotTime) {
      latestSnapshotTime = updatePacket.time;
      return true;
    }
    return false;
  };

  let resetSnapshotTime = () => {
    latestSnapshotTime = -1;
  };

  let updateBrokerMapping = (req) => {
    let updated = isNewSnapshot(req.data);
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

  let agcWorkerStateData = {
    instanceId: options.instanceId,
    instanceIp: options.instanceIp,
    instanceIpFamily: options.instanceIpFamily || 'IPv4'
  };

  let emitAGCWorkerJoinCluster = async () => {
    let data;
    try {
      data = await stateSocket.invoke('agcWorkerJoinCluster', agcWorkerStateData);
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

  let clusterMessageHandler = (channelName, packet) => {
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
      clusterClient.subscribe(channel);
    }
  })();

  (async () => {
    for await (let {channel} of broker.listener('unsubscribe')) {
      clusterClient.unsubscribe(channel);
    }
  })();

  let publishOutboundBuffer = {};
  let publishTimeout = null;

  let flushPublishOutboundBuffer = () => {
    Object.keys(publishOutboundBuffer).forEach((channelName) => {
      let packet = {
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
        let packet = {
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
