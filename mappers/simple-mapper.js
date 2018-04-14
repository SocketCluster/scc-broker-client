var Hasher = require('../hasher');

function SimpleMapper() {
  this.hasher = new Hasher();
  this.sites = [];
}

SimpleMapper.prototype.setSites = function (sites) {
  this.sites = sites;
};

SimpleMapper.prototype.getSites = function () {
  return this.sites;
};

SimpleMapper.prototype.findSite = function (key) {
  var sites = this.sites;
  var targetIndex = this.hasher.hash(key, sites.length);
  return sites[targetIndex];
};

module.exports = SimpleMapper;
