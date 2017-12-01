var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var mock = require('mock-require');
var uuid = require('uuid');

var CLUSTER_SCALE_DELAY = 50;

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
  setTimeout(() => {
    sccBrokerSocket.emit('connect');
  }, 10);
  return sccBrokerSocket;
};

var activeClientMap = {};
var sccBrokerPublishHistory = [];
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

    socket.emit = function (event) {
      // Make the EventEmitter behave like a socket by supressing events
      // which are emitted before the socket is connected.
      if (this.state == 'open' || event == 'connect') {
        EventEmitter.prototype.emit.apply(this, arguments);
      }
    };

    socket.once('connect', () => {
      socket.state = 'open';
    });

    socket.id = uuid.v4();

    activeClientMap[uri] = socket;

    socket.subscriberLookup = {};

    socket.disconnect = function () {
      socket.subscriberLookup = {};
    };

    socket.subscribe = function (channelName) {
      socket.subscriberLookup[channelName] = true;
      return true;
    };

    socket.subscriptions = function () {
      return Object.keys(socket.subscriberLookup || []);
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

    socket.unwatch = function (channelName, handler) {
      if (watcherMap[channelName]) {
        watcherMap[channelName] = watcherMap[channelName].filter(function (watcherData) {
          return watcherData.handler !== handler;
        });
      }
    };

    socket.publish = function (channelName, data, callback) {
      setTimeout(() => {
        sccBrokerPublishHistory.push({
          brokerURI: uri,
          channelName: channelName,
          data: data
        });
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
    sccBrokerPublishHistory = [];
    broker = new BrokerStub({
      clusterInstanceIp: '127.0.0.1'
    });
    clusterClient = scClusterBrokerClient.attach(broker, brokerClientOptions);
    done();
  });

  afterEach('Cleanup scClusterBrokerClient', (done) => {
    var subscriptions = clusterClient.getAllSubscriptions();
    subscriptions.forEach((channelName) => {
      clusterClient.unsubscribe(channelName);
    });
    clusterClient.removeAllListeners();

    while (clusterClient.pubMappers.length) {
      clusterClient.pubMapperShift();
    }
    while (clusterClient.subMappers.length) {
      clusterClient.subMapperShift();
    }

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
        assert.equal(JSON.stringify(clusterClient.pubMappers), JSON.stringify([{targets: {}, subscriptions: {}}]));
        assert.equal(JSON.stringify(clusterClient.subMappers), JSON.stringify([{targets: {}, subscriptions: {}}]));
        assert.equal(typeof clusterClient.pubMappers[0].mapper, 'function');
        done();
      }, 100);
    });

    it('Should work correctly with a couple of scc-brokers', (done) => {
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
      }, 120);

      setTimeout(() => {
        broker.emit('subscribe', 'a1');
        broker.emit('subscribe', 'a2');
      }, 250);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(clusterClient.subMappers.length, 1);
        for (var i = 0; i < 100; i++) {
          broker.emit('publish', 'a' + i, `Message from a${i} channel`);
        }
      }, 330);

      setTimeout(() => {
        assert.equal(errors.length, 0);

        var sccBrokerCount1 = 0;
        var sccBrokerCount2 = 0;
        sccBrokerPublishHistory.forEach((messageData) => {
          if (messageData.brokerURI === 'wss://scc-broker-1:8888') {
            sccBrokerCount1++;
          } else if (messageData.brokerURI === 'wss://scc-broker-2:8888') {
            sccBrokerCount2++;
          }
        });

        var countSum = sccBrokerCount1 + sccBrokerCount2;
        var countDiff = Math.abs(sccBrokerCount1 - sccBrokerCount2);

        // Check if the distribution between scc-brokers is roughly even.
        // That is, the difference is less than 10% of the sum of all messages.
        assert.equal(countDiff / countSum < 0.1, true);

        assert.equal(receivedMessages.length, 2);
        assert.equal(receivedMessages[0].channelName, 'a1');
        assert.equal(receivedMessages[0].data, 'Message from a1 channel');

        assert.equal(receivedMessages[1].channelName, 'a2');
        assert.equal(receivedMessages[1].data, 'Message from a2 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 370);
    });

    it('Should work correctly while adding new brokers', (done) => {
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
        broker.emit('subscribe', 'a2');
      }, 250);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(clusterClient.subMappers.length, 1);
        for (var i = 0; i < 100; i++) {
          broker.emit('publish', 'a' + i, `Message from a${i} channel`);
        }
      }, 330);

      setTimeout(() => {
        assert.equal(errors.length, 0);

        var sccBrokerCount1 = 0;
        var sccBrokerCount2 = 0;
        sccBrokerPublishHistory.forEach((messageData) => {
          if (messageData.brokerURI === 'wss://scc-broker-1:8888') {
            sccBrokerCount1++;
          } else if (messageData.brokerURI === 'wss://scc-broker-2:8888') {
            sccBrokerCount2++;
          }
        });

        var countSum = sccBrokerCount1 + sccBrokerCount2;
        var countDiff = Math.abs(sccBrokerCount1 - sccBrokerCount2);

        // Check if the distribution between scc-brokers is roughly even.
        // That is, the difference is less than 10% of the sum of all messages.
        assert.equal(countDiff / countSum < 0.1, true);

        assert.equal(receivedMessages.length, 2);
        assert.equal(receivedMessages[0].channelName, 'a1');
        assert.equal(receivedMessages[0].data, 'Message from a1 channel');

        assert.equal(receivedMessages[1].channelName, 'a2');
        assert.equal(receivedMessages[1].data, 'Message from a2 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 370);
    });
  });

  // describe('Unstable network', () => {
  //
  // });
});
