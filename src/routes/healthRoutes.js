const express = require('express');
const router = express.Router();
const { getPoolStatus } = require('../config/database');
const { getProcessedEvents } = require('../kafka/orderConsumer');

/**
 * GET /api/v1/health
 */
router.get('/', (req, res) => {
  res.json({
    status: 'UP',
    service: 'Multi-Tenant SaaS Backend',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

/**
 * GET /api/v1/health/pool
 * Lecture 17 Requirement: Connection Pooling Monitoring
 */
router.get('/pool', async (req, res, next) => {
  try {
    const poolStatus = await getPoolStatus();
    res.json({
      status: 'HEALTHY',
      description: 'PostgreSQL Database Connection Pool Status (pg-pool)',
      pool: poolStatus
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/v1/health/kafka
 * Lecture 18 Requirement: Kafka Consumer Group and Offset Inspection
 */
router.get('/kafka', (req, res) => {
  const events = getProcessedEvents();
  res.json({
    status: 'ACTIVE',
    consumerGroup: 'order-processing-group',
    topic: 'tenant.orders',
    totalEventsProcessed: events.length,
    recentOffsets: events.slice(-10)
  });
});

module.exports = router;
