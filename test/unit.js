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

  beforeEach('BEFORE EACH', (done) => {
    clusterClient = scClusterBrokerClient.attach(broker, brokerClientOptions);
    done();
  });

  describe('Without failure', () => {
    it('should initiate correctly without any scc-brokers', (done) => {
      sccStateSocket.on('clientJoinCluster', (stateSocketData, callback) => {
        callback(null, {
          serverInstances: ['wss://scc-broker-1:8888', 'wss://scc-broker-2:8888'],
          time: Date.now()
        });
      });
      sccStateSocket.on('clientSetState', (data, callback) => {
        callback();
        setTimeout(() => {
          sccStateSocket.emit('clientStatesConverge', {state: data.instanceState});
        }, 0);
      });
      setTimeout(() => {
        assert.equal(clusterClient.pubMappers.length, 0);
        assert.equal(clusterClient.subMappers.length, 0);
        done();
      }, 100);
    });
  });
});
