const express = require('express');
const cors = require('cors');
const pool = require('./config/db');
const migrate = require('./db/migrate');
const { init: initKafkaProducer } = require('./services/kafkaProducer');
const errorHandler = require('./middleware/errorHandler');

const app = express();

app.set('trust proxy', 1);
app.use(cors());
app.use(express.json());

// API routes first — order matters so /:shortCode doesn't shadow /api/*
app.use('/api/shorten', require('./routes/shorten'));
app.use('/api/analytics', require('./routes/analytics'));
// Catch-all redirect route last
app.use('/', require('./routes/redirect'));
app.use(errorHandler);

async function waitForPostgres(retries = 20, delayMs = 3000) {
  for (let i = 0; i < retries; i++) {
    try {
      const client = await pool.connect();
      client.release();
      console.log('PostgreSQL connected');
      return;
    } catch (err) {
      console.log(`PostgreSQL not ready (${i + 1}/${retries})… retrying in ${delayMs}ms`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error('Could not connect to PostgreSQL after multiple retries');
}

async function start() {
  await waitForPostgres();
  await migrate();
  await initKafkaProducer();

  const port = process.env.PORT || 4000;
  app.listen(port, () => console.log(`Server listening on port ${port}`));
}

start().catch((err) => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
