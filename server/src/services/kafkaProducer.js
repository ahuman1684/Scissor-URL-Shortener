const kafka = require('../config/kafka');

let producer = null;

async function init() {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic: 'click-events', numPartitions: 3, replicationFactor: 1 }],
    waitForLeaders: true,
  });
  await admin.disconnect();
  console.log('Kafka topic ready');

  producer = kafka.producer();
  await producer.connect();
  console.log('Kafka producer connected');
}

function publishClickEvent(event) {
  if (!producer) return;
  // Fire-and-forget — intentional for redirect latency
  producer.send({
    topic: 'click-events',
    messages: [{ value: JSON.stringify(event) }],
  }).catch((err) => console.error('Kafka publish error:', err.message));
}

module.exports = { init, publishClickEvent };
