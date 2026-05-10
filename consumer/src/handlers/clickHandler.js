const axios = require('axios');
const pool = require('../config/db');

// Local cache to avoid duplicate IP → country lookups
const ipCache = new Map();

async function getCountry(ip) {
  if (!ip || ip === '127.0.0.1' || ip === '::1' || ip.startsWith('172.') || ip.startsWith('10.')) {
    return null;
  }
  if (ipCache.has(ip)) return ipCache.get(ip);

  try {
    const { data } = await axios.get(`http://ip-api.com/json/${ip}`, { timeout: 3000 });
    const country = data.status === 'success' ? data.country : null;
    ipCache.set(ip, country);
    return country;
  } catch {
    return null;
  }
}

async function handleClick(message) {
  const event = JSON.parse(message.value.toString());
  const { shortCode, clickedAt, ipAddress, userAgent, referrer } = event;

  const country = await getCountry(ipAddress);
  const statDate = clickedAt.split('T')[0]; // 'YYYY-MM-DD'

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `INSERT INTO click_events (short_code, clicked_at, ip_address, user_agent, referrer, country)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [shortCode, clickedAt, ipAddress, userAgent, referrer, country]
    );

    // Upsert daily aggregation
    await client.query(
      `INSERT INTO daily_stats (short_code, stat_date, click_count)
       VALUES ($1, $2, 1)
       ON CONFLICT (short_code, stat_date)
       DO UPDATE SET click_count = daily_stats.click_count + 1`,
      [shortCode, statDate]
    );

    // Increment denormalized counter on urls table
    await client.query(
      'UPDATE urls SET click_count = click_count + 1 WHERE short_code = $1',
      [shortCode]
    );

    await client.query('COMMIT');
    console.log(`Processed click for ${shortCode}`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { handleClick };
