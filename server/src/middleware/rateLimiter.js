const requests = new Map();

// Prune stale entries every minute to prevent unbounded memory growth
setInterval(() => {
  const cutoff = Date.now() - 60000;
  for (const [ip, times] of requests.entries()) {
    const fresh = times.filter((t) => t > cutoff);
    if (fresh.length === 0) requests.delete(ip);
    else requests.set(ip, fresh);
  }
}, 60000);

function rateLimiter(limit, windowMs) {
  return (req, res, next) => {
    const ip = req.ip || req.socket.remoteAddress;
    const now = Date.now();
    const windowStart = now - windowMs;

    const times = (requests.get(ip) || []).filter((t) => t > windowStart);
    if (times.length >= limit) {
      return res.status(429).json({ error: 'Rate limit exceeded. Try again in a minute.' });
    }
    times.push(now);
    requests.set(ip, times);
    next();
  };
}

module.exports = rateLimiter;
