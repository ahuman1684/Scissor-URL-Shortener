const pool = require('../config/db');

async function getAnalytics(req, res, next) {
  try {
    const { shortCode } = req.params;

    const urlResult = await pool.query(
      'SELECT original_url, click_count, created_at FROM urls WHERE short_code = $1',
      [shortCode]
    );

    if (urlResult.rows.length === 0) {
      return res.status(404).json({ error: 'Short URL not found' });
    }

    const url = urlResult.rows[0];

    const [dailyResult, referrersResult, countriesResult] = await Promise.all([
      pool.query(
        `SELECT stat_date::text AS date, click_count AS clicks
         FROM daily_stats
         WHERE short_code = $1
           AND stat_date >= NOW() - INTERVAL '30 days'
         ORDER BY stat_date`,
        [shortCode]
      ),
      pool.query(
        `SELECT referrer, COUNT(*) AS count
         FROM click_events
         WHERE short_code = $1 AND referrer IS NOT NULL AND referrer != ''
         GROUP BY referrer
         ORDER BY count DESC
         LIMIT 5`,
        [shortCode]
      ),
      pool.query(
        `SELECT country, COUNT(*) AS count
         FROM click_events
         WHERE short_code = $1 AND country IS NOT NULL
         GROUP BY country
         ORDER BY count DESC
         LIMIT 5`,
        [shortCode]
      ),
    ]);

    return res.json({
      shortCode,
      originalUrl: url.original_url,
      totalClicks: parseInt(url.click_count, 10),
      createdAt: url.created_at,
      clicksLast7Days: dailyResult.rows.map((r) => ({
        date: r.date,
        clicks: parseInt(r.clicks, 10),
      })),
      topReferrers: referrersResult.rows.map((r) => ({
        referrer: r.referrer,
        count: parseInt(r.count, 10),
      })),
      topCountries: countriesResult.rows.map((r) => ({
        country: r.country,
        count: parseInt(r.count, 10),
      })),
    });
  } catch (err) {
    next(err);
  }
}

async function getSummary(req, res, next) {
  try {
    const [linksResult, clicksResult, dailyResult, topLinksResult] = await Promise.all([
      pool.query('SELECT COUNT(*) AS total FROM urls'),
      pool.query('SELECT COALESCE(SUM(click_count), 0) AS total FROM urls'),
      pool.query(
        `SELECT stat_date::text AS date, SUM(click_count) AS clicks
         FROM daily_stats
         WHERE stat_date >= NOW() - INTERVAL '14 days'
         GROUP BY stat_date
         ORDER BY stat_date`
      ),
      pool.query(
        `SELECT short_code, original_url, click_count AS clicks
         FROM urls
         ORDER BY click_count DESC
         LIMIT 5`
      ),
    ]);

    return res.json({
      totalLinks: parseInt(linksResult.rows[0].total, 10),
      totalClicks: parseInt(clicksResult.rows[0].total, 10),
      clicksLast7Days: dailyResult.rows.map((r) => ({
        date: r.date,
        clicks: parseInt(r.clicks, 10),
      })),
      topLinks: topLinksResult.rows.map((r) => ({
        shortCode: r.short_code,
        originalUrl: r.original_url,
        clicks: parseInt(r.clicks, 10),
      })),
    });
  } catch (err) {
    next(err);
  }
}

module.exports = { getAnalytics, getSummary };
