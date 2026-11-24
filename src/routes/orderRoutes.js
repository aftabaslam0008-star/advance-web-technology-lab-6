const express = require('express');
const router = express.Router();
const orderService = require('../services/orderService');
const { publishOrderCreatedEvent } = require('../kafka/producer');
const logger = require('../utils/logger');

/**
 * POST /api/v1/orders
 * Lecture 18 Requirement:
 * Immediately responds with 202 Accepted and publishes OrderCreated event to Kafka topic 'tenant.orders'
 * Key = tenant_id to guarantee sequential order delivery within partitions.
 */
router.post('/', async (req, res, next) => {
  try {
    const { items, userId } = req.body;

    if (!items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({
        error: 'INVALID_REQUEST',
        message: 'Order items array is required and cannot be empty.'
      });
    }

    const tenantId = req.tenantId;

    // 1. Create order in PENDING status in tenant-scoped database
    const pendingOrder = await orderService.createPendingOrder({
      tenantId,
      userId: userId || req.user?.userId || null,
      items
    });

    // 2. Publish OrderCreated event to Kafka topic 'tenant.orders' with message key = tenantId
    const kafkaResult = await publishOrderCreatedEvent({
      tenantId,
      orderId: pendingOrder.orderId,
      items: pendingOrder.items,
      totalAmount: pendingOrder.totalAmount
    });

    // 3. Immediately respond with HTTP 202 Accepted
    return res.status(202).json({
      status: 'ACCEPTED',
      message: 'Order placed successfully and queued for asynchronous processing.',
      orderId: pendingOrder.orderId,
      tenantId,
      totalAmount: pendingOrder.totalAmount,
      kafka: {
        topic: kafkaResult.topic,
        partition: kafkaResult.partition,
        offset: kafkaResult.offset,
        key: tenantId,
        eventId: kafkaResult.eventId
      },
      checkStatusUrl: `/api/v1/orders/${pendingOrder.orderId}`
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/v1/orders
 * Retrieves all orders for active tenant
 */
router.get('/', async (req, res, next) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const result = await orderService.getOrdersOptimized(req.tenantId, limit);
    res.json({
      tenantId: req.tenantId,
      count: result.orderCount,
      orders: result.orders
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/v1/orders/:id
 * Retrieves a specific order by ID.
 * Strict Tenant Isolation: Tenant A will receive 404 for Tenant B's order ID.
 */
router.get('/:id', async (req, res, next) => {
  try {
    const order = await orderService.getOrderById(req.tenantId, req.params.id);
    if (!order) {
      return res.status(404).json({
        error: 'ORDER_NOT_FOUND',
        message: `Order '${req.params.id}' was not found for active tenant '${req.tenantId}'.`
      });
    }
    res.json({
      tenantId: req.tenantId,
      order
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
