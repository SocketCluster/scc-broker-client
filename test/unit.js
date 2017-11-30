var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var mock = require('mock-require');

var CLUSTER_SCALE_DELAY = 100;

var brokerClientOptions = {
  stateServerHost: 'scc-state',
  stateServerPort: 7777,
  authKey: 'sampleAuthKey',
  stateServerConnectTimeout: 100,
  stateServerAckTimeout: 100,
  stateServerReconnectRandomness: 0
};

var clusterClient = null;
var sccStateSocket = null;
var sccBrokerSocket = null;
var receivedMessages = [];

function BrokerStub(options) {
  this.options = options;
  this.subscriptions = {};
}

BrokerStub.prototype = Object.create(EventEmitter.prototype);

BrokerStub.prototype.publish = function (channelName, data) {
  // This is an upstream/outbound publish when the message comes from an scc-broker
  // and needs to reach the end clients.
  receivedMessages.push({channelName: channelName, data: data});
};

var broker = new BrokerStub({
  clusterInstanceIp: '127.0.0.1'
});

var connectSCCStateSocket = function (options) {
  sccStateSocket = new EventEmitter();
  return sccStateSocket;
};

var connectSCCBrokerSocket = function (options) {
  sccBrokerSocket = new EventEmitter();
  sccBrokerSocket.subscriptions = function () {
    return [];
  };
  return sccBrokerSocket;
};

var activeClientMap = {};
var watcherMap = {};

mock('socketcluster-client', {
  connect: function (options) {
    var uri = '';
    uri += options.secure ? 'wss://' : 'ws://';
    uri += options.hostname;
    uri += ':' + options.port;

    if (activeClientMap[uri]) {
      return activeClientMap[uri];
    }
    var socket;
    if (options.hostname == brokerClientOptions.stateServerHost) {
      socket = connectSCCStateSocket(options);
    } else {
      socket = connectSCCBrokerSocket(options);
    }
    activeClientMap[uri] = socket;

    socket.subscriberLookup = {};

    socket.disconnect = function () {
      socket.subscriberLookup = {};
    };

    socket.subscribe = function (channelName) {
      socket.subscriberLookup[channelName] = true;
    };

    socket.unsubscribe = function (channelName) {
      delete socket.subscriberLookup[channelName];
    };

    socket.watchers = function (channelName) {
      return watcherMap[channelName] || [];
    };

    socket.watch = function (channelName, handler) {
      if (!watcherMap[channelName]) {
        watcherMap[channelName] = [];
      }
      watcherMap[channelName].push({
        socket: socket,
        handler: handler
      });
    };

    socket.publish = function (channelName, data, callback) {
      setTimeout(() => {
        var watchers = watcherMap[channelName] || [];
        watchers.forEach((watcher) => {
          if (watcher.socket.subscriberLookup[channelName]) {
            watcher.handler(data);
          }
        });

        callback && callback();
      }, 0);
    };
    return socket;
  }
});

var scClusterBrokerClient = require('../index');

describe('Unit tests.', () => {
  beforeEach('Prepare scClusterBrokerClient', (done) => {
    activeClientMap = {};
    watcherMap = {};
    receivedMessages = [];
    broker = new BrokerStub({
      clusterInstanceIp: '127.0.0.1'
    });
    clusterClient = scClusterBrokerClient.attach(broker, brokerClientOptions);
    done();
  });

  afterEach('Cleanup scClusterBrokerClient', (done) => {
    clusterClient.removeAllListeners();
    done();
  });

  describe('Stable network.', () => {
    it('Should initiate correctly without any scc-brokers', (done) => {
      sccStateSocket.on('clientJoinCluster', (stateSocketData, callback) => {
        setTimeout(() => {
          callback(null, {
            serverInstances: [],
            time: Date.now()
          });
        }, 0);
      });
      sccStateSocket.on('clientSetState', (data, callback) => {
        callback();
        setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState}, function () {});
        }, 0);
      });

      sccStateSocket.emit('connect');

      setTimeout(() => {
        // The mappers should contain a single item whose targets should be an empty object.
        assert.equal(JSON.stringify(clusterClient.pubMappers), JSON.stringify([{targets: {}}]));
        assert.equal(JSON.stringify(clusterClient.subMappers), JSON.stringify([{targets: {}}]));
        assert.equal(typeof clusterClient.pubMappers[0].mapper, 'function');
        done();
      }, 100);
    });

    it('Should initiate correctly with a couple of scc-brokers', (done) => {
      var serverInstancesLookup = {};
      var serverInstanceList = [];

      sccStateSocket.on('clientJoinCluster', (stateSocketData, callback) => {
        setTimeout(() => {
          callback(null, {
            serverInstances: serverInstanceList,
            time: Date.now()
          });
        }, 0);
      });
      sccStateSocket.on('clientSetState', (data, callback) => {
        callback();
        setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState}, function () {});
        }, 0);
      });

      var errors = [];
      clusterClient.on('error', (err) => {
        errors.push(err);
      });

      var serverJoinClusterTimeout;

      setTimeout(() => {
        assert.equal(JSON.stringify(clusterClient.pubMappers), JSON.stringify([]));
        assert.equal(JSON.stringify(clusterClient.subMappers), JSON.stringify([]));

        serverInstanceList = ['wss://scc-broker-1:8888'];
        var sccBrokerStateSocketData = {
          serverInstances: serverInstanceList,
          time: Date.now()
        };
        clearTimeout(serverJoinClusterTimeout);
        serverJoinClusterTimeout = setTimeout(() => {
          sccStateSocket.emit('serverJoinCluster', sccBrokerStateSocketData, function () {});
        }, CLUSTER_SCALE_DELAY);
      }, 100);

      setTimeout(() => {
        serverInstanceList = ['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888'];
        var sccBrokerStateSocketData = {
          serverInstances: serverInstanceList,
          time: Date.now()
        };
        clearTimeout(serverJoinClusterTimeout);
        serverJoinClusterTimeout = setTimeout(() => {
          sccStateSocket.emit('serverJoinCluster', sccBrokerStateSocketData, function () {});
        }, CLUSTER_SCALE_DELAY);
      }, 110);

      setTimeout(() => {
        sccStateSocket.emit('connect');
      }, 220);

      setTimeout(() => {
        broker.emit('subscribe', 'a1');
      }, 250);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(clusterClient.subMappers.length, 1);
        broker.emit('publish', 'a1', 'Message from a1 channel');
      }, 300);

      setTimeout(() => {
        assert.equal(errors.length, 0);

        assert.equal(receivedMessages.length, 1);
        assert.equal(receivedMessages[0].channelName, 'a1');
        assert.equal(receivedMessages[0].data, 'Message from a1 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 350);
    });
  });

  // describe('Unstable network', () => {
  //
  // });
});
