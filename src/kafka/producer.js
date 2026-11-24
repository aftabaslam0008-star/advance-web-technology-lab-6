const { v4: uuidv4 } = require('uuid');
const { getKafka, inMemoryBroker } = require('../config/kafka');
const config = require('../config');
const logger = require('../utils/logger');

let kafkaProducer = null;
let isConnected = false;

async function initProducer() {
  if (isConnected) return;

  const kafka = getKafka();
  if (kafka && config.kafka.enabled) {
    try {
      kafkaProducer = kafka.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000
      });
      await kafkaProducer.connect();
      isConnected = true;
      logger.info('Connected to external Apache Kafka Producer broker.');
      return;
    } catch (err) {
      logger.warn('Could not connect to external Kafka broker. Activating embedded in-process Kafka broker.', { error: err.message });
      kafkaProducer = null;
    }
  }

  // Fallback to in-memory Kafka broker
  logger.info('Running with embedded partition-aware Kafka broker for zero-friction evaluation.');
  isConnected = true;
}

/**
 * Order Producer (Lecture 18 Requirement)
 * When an order is placed, publishes an OrderCreated event to Kafka topic 'tenant.orders'
 * Key is strictly set to tenant_id (or order_id) to guarantee sequential order delivery within partitions.
 */
async function publishOrderCreatedEvent(orderPayload) {
  await initProducer();

  const { tenantId, orderId, items, totalAmount } = orderPayload;
  const eventId = `evt_${uuidv4().substring(0, 8)}`;
  const topic = config.kafka.topicOrders;

  const eventMessage = {
    eventId,
    eventType: 'OrderCreated',
    tenantId,
    orderId,
    items,
    totalAmount,
    timestamp: new Date().toISOString()
  };

  const payloadString = JSON.stringify(eventMessage);
  const messageKey = tenantId; // Key = tenant_id guarantees partition stickiness & sequential FIFO delivery

  logger.info('Kafka Producer: Publishing OrderCreated event', {
    topic,
    key: messageKey,
    orderId,
    tenantId
  });

  if (kafkaProducer) {
    const recordMetadata = await kafkaProducer.send({
      topic,
      messages: [
        {
          key: messageKey,
          value: payloadString,
          headers: {
            'x-tenant-id': tenantId,
            'x-event-type': 'OrderCreated'
          }
        }
      ]
    });

    logger.info('Kafka Producer: Event published to external broker', {
      topic,
      partition: recordMetadata[0]?.partition,
      offset: recordMetadata[0]?.offset
    });

    return {
      success: true,
      broker: 'external_kafka',
      topic,
      partition: recordMetadata[0]?.partition,
      offset: recordMetadata[0]?.offset,
      eventId
    };
  }

  // Embedded partition-aware Kafka Broker
  const results = await inMemoryBroker.send({
    topic,
    messages: [
      {
        key: messageKey,
        value: payloadString
      }
    ]
  });

  const res = results[0];
  logger.info('Kafka Producer: Event published to partition', {
    topic,
    partition: res.partition,
    offset: res.offset,
    key: messageKey,
    eventId
  });

  return {
    success: true,
    broker: 'embedded_kafka',
    topic,
    partition: res.partition,
    offset: res.offset,
    eventId
  };
}

module.exports = {
  publishOrderCreatedEvent,
  initProducer
};
