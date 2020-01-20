const socketClusterClient = require('socketcluster-client');
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
      instanceType: 'scc-worker',
      version: packageVersion
    }
  };
  let stateSocket = socketClusterClient.create(scStateSocketOptions);
  (async () => {
    for await (let event of stateSocket.listener('error')) {
      clusterClient.emit('error', event);
    }
  })();

  let latestBrokersSnapshotTime = -1;

  let isNewBrokersSnapshot = (updatePacket) => {
    if (updatePacket.time > latestBrokersSnapshotTime) {
      latestBrokersSnapshotTime = updatePacket.time;
      return true;
    }
    return false;
  };

  let resetBrokersSnapshotTime = () => {
    latestBrokersSnapshotTime = -1;
  };

  let updateBrokerMapping = (req) => {
    let data = req.data || {};
    let updated = isNewBrokersSnapshot(data);
    if (updated) {
      clusterClient.setBrokers(data.sccBrokerURIs);
    }
    req.end();
  };

  let latestWorkersSnapshotTime = -1;

  let isNewWorkersSnapshot = (updatePacket) => {
    if (updatePacket.time > latestWorkersSnapshotTime) {
      latestWorkersSnapshotTime = updatePacket.time;
      return true;
    }
    return false;
  };

  let resetWorkersSnapshotTime = () => {
    latestWorkersSnapshotTime = -1;
  };

  let triggerUpdateWorkers = (data) => {
    clusterClient.emit('updateWorkers', {
      workerURIs: data.sccWorkerURIs,
      sourceWorkerURI: data.sccSourceWorkerURI
    });
  };

  let updateWorkerMapping = (req) => {
    let data = req.data || {};
    let updated = isNewWorkersSnapshot(data);
    if (updated) {
      triggerUpdateWorkers(data);
    }
    req.end();
  };

  (async () => {
    for await (let req of stateSocket.procedure('sccBrokerJoinCluster')) {
      updateBrokerMapping(req);
    }
  })();
  (async () => {
    for await (let req of stateSocket.procedure('sccBrokerLeaveCluster')) {
      updateBrokerMapping(req);
    }
  })();
  (async () => {
    for await (let req of stateSocket.procedure('sccWorkerJoinCluster')) {
      updateWorkerMapping(req);
    }
  })();
  (async () => {
    for await (let req of stateSocket.procedure('sccWorkerLeaveCluster')) {
      updateWorkerMapping(req);
    }
  })();

  let sccWorkerStateData = {
    instanceId: options.instanceId,
    instanceIp: options.instanceIp,
    instanceIpFamily: options.instanceIpFamily || 'IPv4'
  };

  let emitsccWorkerJoinCluster = async () => {
    let data;
    try {
      data = await stateSocket.invoke('sccWorkerJoinCluster', sccWorkerStateData);
    } catch (error) {
      stateSocket.emit('error', {error});
      setTimeout(emitsccWorkerJoinCluster, retryDelay);
      return;
    }
    resetBrokersSnapshotTime();
    resetWorkersSnapshotTime();
    clusterClient.setBrokers(data.sccBrokerURIs);
    triggerUpdateWorkers(data);
  };

  (async () => {
    for await (let event of stateSocket.listener('connect')) {
      emitsccWorkerJoinCluster();
    }
  })();

  let clusterMessageHandler = (channelName, packet) => {
    if ((packet.sender == null || packet.sender !== options.instanceId) && packet.messages && packet.messages.length) {
      packet.messages.forEach((data) => {
        broker.invokePublish(channelName, data, true);
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
      if (clusterClient.isReady) {
        clusterClient.subscribe(channel);
      }
    }
  })();

  (async () => {
    for await (let {channel} of broker.listener('unsubscribe')) {
      if (clusterClient.isReady) {
        clusterClient.unsubscribe(channel);
      }
    }
  })();

  let publishOutboundBuffer = {};
  let publishTimeout = null;

  let flushPublishOutboundBuffer = () => {
    Object.keys(publishOutboundBuffer).forEach(async (channelName) => {
      let packet = {
        sender: options.instanceId || null,
        messages: publishOutboundBuffer[channelName],
      };
      try {
        await clusterClient.invokePublish(channelName, packet);
      } catch (error) {
        clusterClient.emit('error', {error});
      }
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
        (async () => {
          try {
            await clusterClient.invokePublish(channel, packet);
          } catch (error) {
            clusterClient.emit('error', {error});
          }
        })();
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
