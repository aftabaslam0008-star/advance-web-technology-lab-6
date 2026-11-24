const { v4: uuidv4 } = require('uuid');
const { getDatabasePool } = require('../config/database');
const logger = require('../utils/logger');

/**
 * Product & Inventory Management Service
 * All methods strictly enforce tenant boundary
 */
class ProductService {
  /**
   * Retrieves products for active tenant
   */
  async getProducts(tenantId) {
    const pool = getDatabasePool();
    const result = await pool.query(
      `SELECT id, tenant_id, sku, name, description, price, stock, created_at
       FROM products
       WHERE tenant_id = $1
       ORDER BY name ASC`,
      [tenantId]
    );
    return result.rows;
  }

  /**
   * Deduct inventory upon order confirmation (called by Kafka Consumer Group)
   * Prevents race conditions and logs to inventory audit table
   */
  async deductInventory(tenantId, productId, quantity, orderId) {
    const pool = getDatabasePool();
    
    // Check product exists and belongs to tenant with sufficient stock
    const checkRes = await pool.query(
      `SELECT id, name, stock FROM products WHERE id = $1 AND tenant_id = $2`,
      [productId, tenantId]
    );

    if (checkRes.rows.length === 0) {
      throw new Error(`Product ${productId} not found for tenant ${tenantId}`);
    }

    const currentProduct = checkRes.rows[0];
    if (currentProduct.stock < quantity) {
      throw new Error(`Insufficient stock for product ${currentProduct.name}. Available: ${currentProduct.stock}, Requested: ${quantity}`);
    }

    // Deduct stock
    const updateRes = await pool.query(
      `UPDATE products
       SET stock = stock - $1
       WHERE id = $2 AND tenant_id = $3
       RETURNING id, name, stock`,
      [quantity, productId, tenantId]
    );

    // Audit log
    const logId = `inv_log_${uuidv4().substring(0, 8)}`;
    await pool.query(
      `INSERT INTO inventory_logs (id, tenant_id, product_id, delta, reason)
       VALUES ($1, $2, $3, $4, $5)`,
      [logId, tenantId, productId, -quantity, `Order fulfillment: ${orderId}`]
    );

    logger.info('Inventory deducted successfully', {
      tenantId,
      productId,
      deductedQty: quantity,
      remainingStock: updateRes.rows[0].stock,
      orderId
    });

    return updateRes.rows[0];
  }
}

module.exports = new ProductService();
