const express = require('express');
const router = express.Router();
const { getPoolStatus } = require('../config/database');
const { getProcessedEvents } = require('../kafka/orderConsumer');
const { generateInvoice, resetColdState } = require('../../serverless/invoiceWorker');

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
 * GET /api/v1/health/serverless
 * Triggers Serverless Cold-Start and Warm-Start benchmarks and returns data for the UI
 */
router.get('/serverless', async (req, res, next) => {
  try {
    resetColdState();
    const testOrderId = `ord_dash_${Date.now()}`;
    const payload = {
      tenantId: 'tenant-alpha',
      orderId: testOrderId,
      totalAmount: 499.00,
      items: [{ productId: 'Enterprise Cloud Server (ALPH-SRV-01)', quantity: 1, unitPrice: 499.00, subtotal: 499.00 }]
    };

    // Cold invocation
    const coldStart = await generateInvoice(payload);

    // Warm invocations
    const warmTimes = [];
    for (let i = 0; i < 4; i++) {
      const warm = await generateInvoice(payload);
      warmTimes.push(warm.executionTimeMs);
    }

    const warmAverageMs = Number((warmTimes.reduce((a, b) => a + b, 0) / warmTimes.length).toFixed(2));
    const speedup = (coldStart.executionTimeMs / (warmAverageMs || 1)).toFixed(2);
    const overheadReduction = (
      ((coldStart.executionTimeMs - warmAverageMs) / (coldStart.executionTimeMs || 1)) * 100
    ).toFixed(2);

    res.json({
      coldStart,
      warmAverageMs,
      warmTimes,
      speedup,
      overheadReduction
    });
  } catch (err) {
    next(err);
  }
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

