const pool = require('../config/db');
const { generateId } = require('../services/idGenerator');
const { setUrl } = require('../services/cacheService');
const validator = require('validator');

async function shorten(req, res, next) {
  try {
    const { originalUrl, customCode, expiresIn } = req.body;

    if (!originalUrl || !validator.isURL(String(originalUrl), { require_protocol: true })) {
      return res.status(400).json({ error: 'Invalid URL — must include http:// or https://' });
    }

    if (customCode !== undefined) {
      if (customCode.length > 20 || !/^[a-zA-Z0-9-]+$/.test(customCode)) {
        return res.status(400).json({
          error: 'Custom code must be alphanumeric with hyphens only, max 20 characters',
        });
      }
    }

    const shortCode = customCode || generateId();
    const expiresAt = expiresIn ? new Date(Date.now() + Number(expiresIn) * 86400000) : null;

    const result = await pool.query(
      `INSERT INTO urls (short_code, original_url, expires_at)
       VALUES ($1, $2, $3)
       RETURNING short_code, original_url, created_at, expires_at`,
      [shortCode, originalUrl, expiresAt]
    );

    const row = result.rows[0];
    await setUrl(shortCode, originalUrl);

    return res.status(201).json({
      shortCode: row.short_code,
      shortUrl: `${process.env.BASE_URL || 'http://localhost:4000'}/${row.short_code}`,
      originalUrl: row.original_url,
      expiresAt: row.expires_at,
      createdAt: row.created_at,
    });
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Short code already exists' });
    }
    next(err);
  }
}

module.exports = { shorten };
