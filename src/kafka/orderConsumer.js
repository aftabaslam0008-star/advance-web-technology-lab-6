const { getKafka, inMemoryBroker } = require('../config/kafka');
const config = require('../config');
const productService = require('../services/productService');
const orderService = require('../services/orderService');
const { generateInvoice } = require('../../serverless/invoiceWorker');
const logger = require('../utils/logger');

let kafkaConsumer = null;
let isConsumerRunning = false;

// Processed messages audit log for observability and offset verification
const processedEvents = [];

/**
 * Background Kafka Consumer Group (Lecture 18 Requirement)
 * Group ID: 'order-processing-group'
 * Subscribes to 'tenant.orders' topic
 * Sequentially consumes events, decrements inventory, confirms order, and logs offsets
 */
async function startOrderConsumer() {
  if (isConsumerRunning) return;

  const kafka = getKafka();
  if (kafka && config.kafka.enabled) {
    try {
      kafkaConsumer = kafka.consumer({
        groupId: config.kafka.groupId
      });

      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({
        topic: config.kafka.topicOrders,
        fromBeginning: false
      });

      await kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await processMessage({
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString() : null,
            value: message.value.toString()
          });
        }
      });

      isConsumerRunning = true;
      logger.info('Kafka Consumer Group connected and listening on topic', {
        topic: config.kafka.topicOrders,
        groupId: config.kafka.groupId
      });
      return;
    } catch (err) {
      logger.warn('Could not start external Kafka consumer group, using embedded consumer group', { error: err.message });
      kafkaConsumer = null;
    }
  }

  // Register on embedded Kafka Broker
  inMemoryBroker.registerConsumer(async ({ topic, partition, message }) => {
    await processMessage({
      topic,
      partition,
      offset: message.offset,
      key: message.key ? message.key.toString() : null,
      value: message.value.toString()
    });
  });

  isConsumerRunning = true;
  logger.info('Embedded Kafka Consumer Group registered and actively listening for events', {
    topic: config.kafka.topicOrders,
    groupId: config.kafka.groupId
  });
}

/**
 * Message processor: Asynchronously handles OrderCreated event
 */
async function processMessage({ topic, partition, offset, key, value }) {
  const startTime = Date.now();
  let eventData = null;

  try {
    eventData = JSON.parse(value);
  } catch (err) {
    logger.error('Failed to parse Kafka message JSON', { offset, error: err.message });
    return;
  }

  const { eventId, eventType, tenantId, orderId, items, totalAmount } = eventData;

  logger.info('[KAFKA CONSUMER] Event Received', {
    topic,
    partition,
    offset,
    messageKey: key,
    eventType,
    tenantId,
    orderId
  });

  if (eventType === 'OrderCreated') {
    try {
      // 1. Asynchronous Inventory Deduction
      for (const item of items) {
        await productService.deductInventory(tenantId, item.productId, item.quantity, orderId);
      }

      // 2. Transition Order Status from PENDING -> CONFIRMED
      const confirmedOrder = await orderService.confirmOrder(tenantId, orderId);

      // 3. Dispatch Async Serverless Worker (PDF Invoice Generation)
      const invoicePayload = {
        tenantId,
        orderId,
        totalAmount,
        items,
        confirmedAt: confirmedOrder.updated_at
      };

      // Call the Serverless Invoice Worker asynchronously
      setImmediate(async () => {
        try {
          const invoiceResult = await generateInvoice({ body: invoicePayload });
          logger.info('[ASYNC SERVERLESS WORKER] Invoice generated successfully', {
            orderId,
            tenantId,
            invoiceNumber: invoiceResult.invoiceNumber,
            durationMs: invoiceResult.executionTimeMs,
            isColdStart: invoiceResult.isColdStart
          });
        } catch (workerErr) {
          logger.error('Error executing serverless invoice worker', { error: workerErr.message });
        }
      });

      const processingLatencyMs = Date.now() - startTime;

      // 4. Log Offset and Record Event Audit
      const auditRecord = {
        topic,
        partition,
        offset: Number(offset),
        key,
        tenantId,
        orderId,
        status: 'PROCESSED_CONFIRMED',
        processingLatencyMs,
        processedAt: new Date().toISOString()
      };

      processedEvents.push(auditRecord);

      logger.info('[KAFKA CONSUMER] Order Processed & Offset Committed Successfully', auditRecord);
    } catch (processError) {
      logger.error('[KAFKA CONSUMER] Failed to process order event', {
        orderId,
        tenantId,
        error: processError.message
      });
    }
  }
}

function getProcessedEvents() {
  return processedEvents;
}

module.exports = {
  startOrderConsumer,
  getProcessedEvents
};
