const { v4: uuidv4 } = require('uuid');
const { getDatabasePool } = require('../config/database');
const logger = require('../utils/logger');

class OrderService {
  /**
   * Creates an order with status 'PENDING'
   * Tenant isolation guaranteed: all inserts bind the active tenant_id
   */
  async createPendingOrder({ tenantId, userId, items }) {
    const pool = getDatabasePool();
    const orderId = `ord_${uuidv4().substring(0, 8)}`;

    // Validate items and calculate total
    let totalAmount = 0;
    const itemsToInsert = [];

    for (const item of items) {
      // Security check: Verify product belongs strictly to active tenant
      const prodRes = await pool.query(
        `SELECT id, name, price, stock FROM products WHERE id = $1 AND tenant_id = $2`,
        [item.productId, tenantId]
      );

      if (prodRes.rows.length === 0) {
        throw new Error(`Product '${item.productId}' does not exist for tenant '${tenantId}' (Tenant boundary violation prevented).`);
      }

      const product = prodRes.rows[0];
      const unitPrice = Number(product.price);
      const quantity = parseInt(item.quantity, 10);
      const subtotal = Number((unitPrice * quantity).toFixed(2));
      totalAmount += subtotal;

      itemsToInsert.push({
        id: `item_${uuidv4().substring(0, 8)}`,
        productId: product.id,
        quantity,
        unitPrice,
        subtotal
      });
    }

    totalAmount = Number(totalAmount.toFixed(2));

    // 1. Insert Order
    await pool.query(
      `INSERT INTO orders (id, tenant_id, user_id, status, total_amount, created_at, updated_at)
       VALUES ($1, $2, $3, 'PENDING', $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [orderId, tenantId, userId || null, totalAmount]
    );

    // 2. Insert Order Items
    for (const item of itemsToInsert) {
      await pool.query(
        `INSERT INTO order_items (id, tenant_id, order_id, product_id, quantity, unit_price, subtotal)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [item.id, tenantId, orderId, item.productId, item.quantity, item.unitPrice, item.subtotal]
      );
    }

    logger.info('Pending order created in database', {
      orderId,
      tenantId,
      itemsCount: itemsToInsert.length,
      totalAmount
    });

    return {
      orderId,
      tenantId,
      status: 'PENDING',
      totalAmount,
      items: itemsToInsert
    };
  }

  /**
   * Retrieves single order by ID with tenant boundary check
   * Tenant A will NEVER receive Tenant B's order (returns null -> 404)
   */
  async getOrderById(tenantId, orderId) {
    const pool = getDatabasePool();
    const orderRes = await pool.query(
      `SELECT id, tenant_id, user_id, status, total_amount, created_at, updated_at
       FROM orders
       WHERE id = $1 AND tenant_id = $2`,
      [orderId, tenantId]
    );

    if (orderRes.rows.length === 0) {
      return null;
    }

    const order = orderRes.rows[0];
    const itemsRes = await pool.query(
      `SELECT oi.id, oi.product_id, p.name as product_name, oi.quantity, oi.unit_price, oi.subtotal
       FROM order_items oi
       JOIN products p ON oi.product_id = p.id AND oi.tenant_id = p.tenant_id
       WHERE oi.order_id = $1 AND oi.tenant_id = $2`,
      [orderId, tenantId]
    );

    order.items = itemsRes.rows;
    return order;
  }

  /**
   * Confirms order status (called by Kafka Consumer after successful inventory deduction)
   */
  async confirmOrder(tenantId, orderId) {
    const pool = getDatabasePool();
    const result = await pool.query(
      `UPDATE orders
       SET status = 'CONFIRMED', updated_at = CURRENT_TIMESTAMP
       WHERE id = $1 AND tenant_id = $2
       RETURNING id, tenant_id, status, updated_at`,
      [orderId, tenantId]
    );
    return result.rows[0];
  }

  /**
   * Lecture 17 Requirement: Un-optimized N+1 Query
   * Fetches orders (1 query) and then fetches items for each order in a loop (N queries)
   * Total queries = 1 + N
   */
  async getOrdersNPlusOne(tenantId, limit = 50) {
    const pool = getDatabasePool();
    const startTime = performance.now();
    let queryCount = 0;

    // 1st Query: Fetch orders
    const ordersResult = await pool.query(
      `SELECT id, tenant_id, user_id, status, total_amount, created_at
       FROM orders
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );
    queryCount++;

    const orders = ordersResult.rows;

    // N Queries: Fetch items individually for each order
    for (const order of orders) {
      const itemsResult = await pool.query(
        `SELECT id, product_id, quantity, unit_price, subtotal
         FROM order_items
         WHERE order_id = $1 AND tenant_id = $2`,
        [order.id, tenantId]
      );
      queryCount++;
      order.items = itemsResult.rows;
    }

    const durationMs = Number((performance.now() - startTime).toFixed(2));

    return {
      type: 'N+1 Un-Optimized Query Pattern',
      tenantId,
      orderCount: orders.length,
      queryCount,
      durationMs,
      orders
    };
  }

  /**
   * Lecture 17 Requirement: Optimized Query using Single JOIN and JSON Aggregation
   * Fetches all orders and their items in ONE SINGLE QUERY (1 query vs 1+N)
   */
  async getOrdersOptimized(tenantId, limit = 50) {
    const pool = getDatabasePool();
    const startTime = performance.now();

    const sql = `
      SELECT 
        o.id,
        o.tenant_id,
        o.user_id,
        o.status,
        o.total_amount,
        o.created_at,
        COALESCE(
          json_agg(
            json_build_object(
              'id', oi.id,
              'product_id', oi.product_id,
              'quantity', oi.quantity,
              'unit_price', oi.unit_price,
              'subtotal', oi.subtotal
            )
          ) FILTER (WHERE oi.id IS NOT NULL), '[]'
        ) as items
      FROM orders o
      LEFT JOIN order_items oi 
        ON o.id = oi.order_id AND o.tenant_id = oi.tenant_id
      WHERE o.tenant_id = $1
      GROUP BY o.id, o.tenant_id, o.user_id, o.status, o.total_amount, o.created_at
      ORDER BY o.created_at DESC
      LIMIT $2;
    `;

    const result = await pool.query(sql, [tenantId, limit]);
    const durationMs = Number((performance.now() - startTime).toFixed(2));

    return {
      type: 'Optimized Single JOIN Query with json_agg',
      tenantId,
      orderCount: result.rows.length,
      queryCount: 1,
      durationMs,
      orders: result.rows
    };
  }

  /**
   * Lecture 17 Requirement: EXPLAIN ANALYZE Execution Plan
   * Runs EXPLAIN ANALYZE on tenant order queries before and after composite indexing
   */
  async runExplainAnalyze(tenantId) {
    const pool = getDatabasePool();
    const sql = `
      EXPLAIN ANALYZE 
      SELECT * FROM orders 
      WHERE tenant_id = $1 
      ORDER BY created_at DESC 
      LIMIT 100;
    `;

    const result = await pool.query(sql, [tenantId]);
    return result.rows.map(r => r['QUERY PLAN'] || Object.values(r)[0]).join('\n');
  }
}

module.exports = new OrderService();
