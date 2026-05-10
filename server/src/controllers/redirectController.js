const pool = require('../config/db');
const { getUrl, setUrl } = require('../services/cacheService');
const { publishClickEvent } = require('../services/kafkaProducer');

async function redirect(req, res, next) {
  try {
    const { shortCode } = req.params;

    let originalUrl = await getUrl(shortCode);

    if (originalUrl) {
      // Cache hit — refresh TTL (LRU approximation)
      setUrl(shortCode, originalUrl).catch(() => {});
    } else {
      // Cache miss — query DB
      const result = await pool.query(
        'SELECT original_url, expires_at FROM urls WHERE short_code = $1',
        [shortCode]
      );

      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'Short URL not found' });
      }

      const row = result.rows[0];

      if (row.expires_at && new Date(row.expires_at) < new Date()) {
        return res.status(410).json({ error: 'This link has expired' });
      }

      originalUrl = row.original_url;
      setUrl(shortCode, originalUrl).catch(() => {});
    }

    // Fire-and-forget — do not await (keeps redirect latency tight)
    publishClickEvent({
      shortCode,
      clickedAt: new Date().toISOString(),
      ipAddress: req.ip || req.socket.remoteAddress,
      userAgent: req.headers['user-agent'] || '',
      referrer: req.headers['referer'] || '',
    });

    return res.redirect(302, originalUrl);
  } catch (err) {
    next(err);
  }
}

module.exports = { redirect };
