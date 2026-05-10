const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'scissor-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: {
    retries: 20,
    initialRetryTime: 1000,
    maxRetryTime: 15000,
  },
});

module.exports = kafka;
