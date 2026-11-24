const { Kafka, logLevel } = require('kafkajs');
const EventEmitter = require('events');
const config = require('./index');
const logger = require('../utils/logger');

let kafkaInstance = null;
let producerInstance = null;
let consumerInstance = null;

/**
 * In-Process Kafka Emulator
 * Emulates Apache Kafka Broker partitioned topics, consumer groups, offset tracking,
 * and key-based sequential ordering for local testing/offline demonstration.
 */
class InMemoryKafkaBroker extends EventEmitter {
  constructor() {
    super();
    this.partitions = { 0: [], 1: [], 2: [] };
    this.offsets = { 0: 0, 1: 0, 2: 0 };
    this.consumerOffsets = { 0: 0, 1: 0, 2: 0 };
    this.consumerHandlers = [];
    this.isRunning = false;
  }

  // Consistent hashing for partition key
  hashKey(key) {
    if (!key) return 0;
    let hash = 0;
    const str = String(key);
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash) % 3;
  }

  async send({ topic, messages }) {
    const results = [];
    for (const msg of messages) {
      const partition = this.hashKey(msg.key);
      const offset = this.offsets[partition]++;
      const record = {
        topic,
        partition,
        offset,
        key: msg.key,
        value: msg.value,
        timestamp: Date.now()
      };
      this.partitions[partition].push(record);
      results.push(record);

      // Async dispatch to consumer group handlers
      setImmediate(() => this.dispatchRecord(record));
    }
    return results;
  }

  async dispatchRecord(record) {
    for (const handler of this.consumerHandlers) {
      try {
        await handler({
          topic: record.topic,
          partition: record.partition,
          message: {
            key: Buffer.from(record.key || ''),
            value: Buffer.from(record.value || ''),
            offset: record.offset.toString(),
            timestamp: record.timestamp.toString()
          }
        });
        this.consumerOffsets[record.partition] = record.offset + 1;
      } catch (err) {
        logger.error('Error in Kafka consumer message handler', { error: err.message });
      }
    }
  }

  registerConsumer(handler) {
    this.consumerHandlers.push(handler);
    this.isRunning = true;
  }
}

const inMemoryBroker = new InMemoryKafkaBroker();

function getKafka() {
  if (!kafkaInstance && config.kafka.enabled) {
    try {
      kafkaInstance = new Kafka({
        clientId: config.kafka.clientId,
        brokers: config.kafka.brokers,
        ssl: config.kafka.ssl,
        sasl: config.kafka.sasl,
        logLevel: logLevel.NOTHING,
        retry: {
          retries: 2,
          initialRetryTime: 300
        }
      });
    } catch (err) {
      logger.warn('Failed to configure external Kafka client, falling back to embedded Kafka event bus', { error: err.message });
      kafkaInstance = null;
    }
  }
  return kafkaInstance;
}

module.exports = {
  getKafka,
  inMemoryBroker
};
