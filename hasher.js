function Hasher() {}

Hasher.prototype.hash = function (key, modulo) {
  var ch;
  var hash = key;

  for (var i = 0; i < key.length; i++) {
    ch = key.charCodeAt(i);
    hash = ((hash << 5) - hash) + ch;
    hash = hash & hash;
  }
  return Math.abs(hash || 0) % modulo;
};

module.exports = Hasher;
