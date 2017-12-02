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
  stateServerReconnectRandomness: 0,
  noErrorLogging: true
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

  describe('Basic cases.', () => {
    it('Should initiate correctly without any scc-brokers', (done) => {
      sccStateSocket.on('clientJoinCluster', (stateSocketData, callback) => {
        setTimeout(() => {
          callback(null, {
            serverInstances: [],
            time: Date.now()
          });
        }, 0);
      });
      var stateConvergenceTimeout;
      sccStateSocket.on('clientSetState', (data, callback) => {
        data = JSON.parse(JSON.stringify(data));
        callback();
        clearTimeout(stateConvergenceTimeout);
        stateConvergenceTimeout = setTimeout(() => {
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

      var stateConvergenceTimeout;
      sccStateSocket.on('clientSetState', (data, callback) => {
        data = JSON.parse(JSON.stringify(data));
        callback();
        clearTimeout(stateConvergenceTimeout);
        stateConvergenceTimeout = setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState}, function () {});
        }, 0);
      });

      var errors = [];
      clusterClient.on('error', (err) => {
        errors.push(err);
      });

      var serverJoinClusterTimeout;

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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
        // Simulate the subscription being made on the broker.
        broker.subscriptions['1'] = {};
        for (var i = 0; i < 100; i++) {
          broker.subscriptions['1']['a' + i] = {};
          broker.emit('subscribe', 'a' + i);
        }
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

        assert.equal(receivedMessages.length, 100);
        assert.equal(receivedMessages[0].channelName, 'a0');
        assert.equal(receivedMessages[0].data, 'Message from a0 channel');

        assert.equal(receivedMessages[1].channelName, 'a1');
        assert.equal(receivedMessages[1].data, 'Message from a1 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 370);
    });

    it('Should work correctly during transition period while adding a new broker', (done) => {
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

      var stateConvergenceTimeout;
      sccStateSocket.on('clientSetState', (data, callback) => {
        data = JSON.parse(JSON.stringify(data));

        if (data.instanceState.indexOf('updatedSubs') == 0) {
          assert.equal(clusterClient.pubMappers.length, 1);
          assert.equal(clusterClient.subMappers.length, 2);
        }

        callback();
        clearTimeout(stateConvergenceTimeout);

        // Add a delay before the state converges.
        stateConvergenceTimeout = setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState}, function () {});

          setTimeout(() => {
            if (data.instanceState.indexOf('updatedSubs') == 0) {
              assert.equal(clusterClient.pubMappers.length, 1);
              assert.equal(clusterClient.subMappers.length, 2);
            } else if (data.instanceState.indexOf('updatedPubs') == 0) {
              assert.equal(clusterClient.pubMappers.length, 1);
              assert.equal(clusterClient.subMappers.length, 1);
            } else if (data.instanceState.indexOf('active') == 0) {
              assert.equal(clusterClient.pubMappers.length, 1);
              assert.equal(clusterClient.subMappers.length, 1);
            }
          }, 0);
        }, 100);

      });

      var errors = [];
      clusterClient.on('error', (err) => {
        errors.push(err);
      });

      setTimeout(() => {
        sccStateSocket.emit('connect');
      }, 0);

      setTimeout(() => {
        assert.equal(JSON.stringify(clusterClient.subMappers[0].targets), JSON.stringify({}));
        // Simulate the subscription being made on the broker.
        broker.subscriptions['1'] = {'a0': {}};
        // Because none of the scc-broker instances are online yet, the subscription
        // will not be established immediately, but it should be established eventually (retry mechanism).
        broker.emit('subscribe', 'a0');
      }, 10);

      var serverJoinClusterTimeout;

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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
      }, 10);

      setTimeout(() => {
        // Because none of the scc-broker instances are online yet, this message will
        // not propagate across the cluster.
        broker.emit('publish', 'a0', 'Hi');
      }, 90);

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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
      }, 400);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(clusterClient.subMappers.length, 2);
        // This message will be published during the transition phase while sc instances
        // are still synching to account for the new scc-broker which just joined.
        // It should propagate across the cluster.
        broker.emit('publish', 'a0', 'Hello world 0');
      }, 570);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(clusterClient.subMappers.length, 1);
        // This message will be published after the transition phase has completed
        // and all sc instances are in sync.
        broker.emit('publish', 'a0', 'Message from a0 channel');
      }, 1000);

      setTimeout(() => {
        // Because we tried to subscribe to a channel before any scc-broker instances
        // were available, we expect some errors.
        // Those errors should not prevent the subscription from being made at a later time
        // once the scc-broker instances have been connected and synched.
        assert.equal(errors.length > 0, true);

        assert.equal(receivedMessages.length, 2);
        assert.equal(receivedMessages[0].channelName, 'a0');
        assert.equal(receivedMessages[0].data, 'Hello world 0');

        assert.equal(receivedMessages[1].channelName, 'a0');
        assert.equal(receivedMessages[1].data, 'Message from a0 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 1100);
    });

    it('Should work correctly after one of the brokers crashes', (done) => {
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

      var stateConvergenceTimeout;
      sccStateSocket.on('clientSetState', (data, callback) => {
        data = JSON.parse(JSON.stringify(data));
        callback();
        clearTimeout(stateConvergenceTimeout);
        stateConvergenceTimeout = setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState}, function () {});
        }, 0);
      });

      var errors = [];
      clusterClient.on('error', (err) => {
        errors.push(err);
      });

      var serverJoinClusterTimeout;

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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

      // Simulate scc-state sending back a 'serverJoinCluster' event after a timeout.
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
        // Simulate the subscription being made on the broker.
        broker.subscriptions['1'] = {};
        for (var i = 0; i < 100; i++) {
          broker.subscriptions['1']['a' + i] = {};
          broker.emit('subscribe', 'a' + i);
        }
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

        assert.equal(receivedMessages.length, 100);
        assert.equal(receivedMessages[0].channelName, 'a0');
        assert.equal(receivedMessages[0].data, 'Message from a0 channel');

        assert.equal(receivedMessages[1].channelName, 'a1');
        assert.equal(receivedMessages[1].data, 'Message from a1 channel');

        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 370);
    });
  });
});
