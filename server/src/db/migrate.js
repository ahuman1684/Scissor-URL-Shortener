const pool = require('../config/db');

const schema = `
  CREATE TABLE IF NOT EXISTS urls (
    id            BIGSERIAL PRIMARY KEY,
    short_code    VARCHAR(12) UNIQUE NOT NULL,
    original_url  TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    expires_at    TIMESTAMPTZ,
    click_count   BIGINT DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS idx_urls_short_code ON urls(short_code);

  CREATE TABLE IF NOT EXISTS click_events (
    id            BIGSERIAL PRIMARY KEY,
    short_code    VARCHAR(12) NOT NULL,
    clicked_at    TIMESTAMPTZ NOT NULL,
    ip_address    VARCHAR(45),
    user_agent    TEXT,
    referrer      TEXT,
    country       VARCHAR(60)
  );

  CREATE INDEX IF NOT EXISTS idx_clicks_short_code ON click_events(short_code);
  CREATE INDEX IF NOT EXISTS idx_clicks_clicked_at ON click_events(clicked_at);

  CREATE TABLE IF NOT EXISTS daily_stats (
    short_code    VARCHAR(12) NOT NULL,
    stat_date     DATE NOT NULL,
    click_count   INT DEFAULT 0,
    PRIMARY KEY (short_code, stat_date)
  );
`;

async function migrate() {
  const client = await pool.connect();
  try {
    await client.query(schema);
    console.log('Database migrations complete');
  } finally {
    client.release();
  }
}

module.exports = migrate;
