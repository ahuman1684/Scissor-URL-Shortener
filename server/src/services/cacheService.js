const redis = require('../config/redis');

const PREFIX = 'url:';
const TTL = 86400; // 24 hours

async function getUrl(shortCode) {
  return redis.get(PREFIX + shortCode);
}

async function setUrl(shortCode, url) {
  await redis.set(PREFIX + shortCode, url, 'EX', TTL);
}

async function invalidate(shortCode) {
  await redis.del(PREFIX + shortCode);
}

module.exports = { getUrl, setUrl, invalidate };
