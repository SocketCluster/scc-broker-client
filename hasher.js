const crypto = require('crypto');

function Hasher() {}

Hasher.prototype.hashToIndex = function (key, modulo) {
  key = key || 0;
  let ch;
  let hash = key;
  for (let i = 0; i < key.length; i++) {
    ch = key.charCodeAt(i);
    hash = ((hash << 5) - hash) + ch;
    hash = hash & hash;
  }
  return Math.abs(hash) % modulo;
};

Hasher.prototype.hashToHex = function (key, algorithm) {
  let hasher = crypto.createHash(algorithm || 'md5');
  hasher.update(key);
  return hasher.digest('hex');
};

module.exports = Hasher;
