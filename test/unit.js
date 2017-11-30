var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var mock = require('mock-require');

var brokerClientOptions = {
  stateServerHost: 'scc-state',
  stateServerPort: 7777,
  authKey: 'sampleAuthKey',
  stateServerConnectTimeout: 100,
  stateServerAckTimeout: 100,
  stateServerReconnectRandomness: 0
};

function BrokerStub(options) {
  this.options = options;
  this.subscriptions = {};
}

BrokerStub.prototype = Object.create(EventEmitter.prototype);

BrokerStub.prototype.publish = function (channelName, data) {
  console.log(222);
};

var broker = new BrokerStub({
  clusterInstanceIp: '127.0.0.1'
});

var clusterClient = null;
var sccStateSocket = null;
var sccBrokerSocket = null;

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

mock('socketcluster-client', {
  connect: function (options) {
    if (options.hostname == brokerClientOptions.stateServerHost) {
      return connectSCCStateSocket(options);
    } else {
      return connectSCCBrokerSocket(options);
    }
  }
});

var scClusterBrokerClient = require('../index');

describe('Unit tests', () => {
  before('BEFORE', (done) => {
    done();
  });

  after('AFTER', (done) => {
    done();
  });

  beforeEach('Prepare scClusterBrokerClient', (done) => {
    clusterClient = scClusterBrokerClient.attach(broker, brokerClientOptions);
    done();
  });

  describe('Simple cases', () => {
    it('should initiate correctly without any scc-brokers', (done) => {
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

    it('should initiate correctly with a couple of scc-brokers', (done) => {
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

      sccStateSocket.emit('connect');

      setTimeout(() => {
        assert.equal(JSON.stringify(clusterClient.pubMappers), JSON.stringify([{targets: {}}]));
        assert.equal(JSON.stringify(clusterClient.subMappers), JSON.stringify([{targets: {}}]));

        serverInstanceList = ['wss://scc-broker-1:8888'];
        var sccBrokerStateSocketData = {
          serverInstances: serverInstanceList,
          time: Date.now()
        };
        sccStateSocket.emit('serverJoinCluster', sccBrokerStateSocketData, function () {});
      }, 100);

      setTimeout(() => {
        serverInstanceList = ['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888'];
        var sccBrokerStateSocketData = {
          serverInstances: serverInstanceList,
          time: Date.now()
        };
        sccStateSocket.emit('serverJoinCluster', sccBrokerStateSocketData, function () {});
      }, 150);

      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.pubMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));

        assert.equal(clusterClient.subMappers.length, 1);
        assert.equal(JSON.stringify(Object.keys(clusterClient.subMappers[0].targets)), JSON.stringify(['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888']));
        done();
      }, 200);
    });
  });
});
