const kafka = require('./config/kafka');
const { handleClick } = require('./handlers/clickHandler');

async function start() {
  const consumer = kafka.consumer({ groupId: 'analytics-consumer-group' });

  await consumer.connect();
  console.log('Kafka consumer connected');

  await consumer.subscribe({ topic: 'click-events', fromBeginning: false });
  console.log('Subscribed to click-events');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        await handleClick(message);
      } catch (err) {
        // Log and move on — idempotency is preferred over blocking on failures
        console.error('Error processing click event:', err.message);
      }
    },
  });
}

start().catch((err) => {
  console.error('Fatal consumer error:', err.message);
  process.exit(1);
});
