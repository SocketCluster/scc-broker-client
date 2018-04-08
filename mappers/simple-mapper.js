function SimpleMapper() {
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
  var ch;
  var hash = key;

  for (var i = 0; i < key.length; i++) {
    ch = key.charCodeAt(i);
    hash = ((hash << 5) - hash) + ch;
    hash = hash & hash;
  }
  var targetIndex = Math.abs(hash || 0) % sites.length;
  return sites[targetIndex];
};

module.exports = SimpleMapper;
