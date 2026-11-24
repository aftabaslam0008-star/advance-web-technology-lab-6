const fs = require('fs');
const path = require('path');
const { getDatabasePool } = require('../config/database');
const logger = require('../utils/logger');

const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

const EMBEDDED_DDL = `
CREATE TABLE IF NOT EXISTS tenants (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  slug VARCHAR(64) UNIQUE NOT NULL,
  subdomain VARCHAR(64) UNIQUE NOT NULL,
  status VARCHAR(32) DEFAULT 'ACTIVE',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  role VARCHAR(32) DEFAULT 'MEMBER',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_tenant_user_email UNIQUE (tenant_id, email)
);

CREATE TABLE IF NOT EXISTS products (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  sku VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  price NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
  stock INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_tenant_product_sku UNIQUE (tenant_id, sku)
);

CREATE TABLE IF NOT EXISTS orders (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  user_id VARCHAR(64) REFERENCES users(id),
  status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  total_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
  idempotency_key VARCHAR(128),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id VARCHAR(64) NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id VARCHAR(64) NOT NULL REFERENCES products(id),
  quantity INT NOT NULL DEFAULT 1,
  unit_price NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
  subtotal NUMERIC(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS inventory_logs (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  product_id VARCHAR(64) NOT NULL REFERENCES products(id),
  delta INT NOT NULL,
  reason VARCHAR(255) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invoices (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id VARCHAR(64) NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  invoice_number VARCHAR(64) NOT NULL,
  pdf_path TEXT,
  status VARCHAR(32) DEFAULT 'GENERATED',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_products_tenant ON products(tenant_id);
CREATE INDEX IF NOT EXISTS idx_orders_tenant ON orders(tenant_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
`;

async function runMigrations() {
  const pool = getDatabasePool();
  logger.info('Running database schema migrations...');

  try {
    let ddl = EMBEDDED_DDL;
    if (fs.existsSync(SCHEMA_PATH)) {
      ddl = fs.readFileSync(SCHEMA_PATH, 'utf-8');
    }
    
    if (pool.exec) {
      await pool.exec(ddl);
    } else {
      await pool.query(ddl);
    }

    logger.info('Schema migrations completed successfully.');
    return true;
  } catch (error) {
    logger.error('Migration failed', { error: error.message });
    throw error;
  }
}

/**
 * Creates the composite index for Lecture 17 optimization benchmarks
 */
async function createCompositeIndex() {
  const pool = getDatabasePool();
  logger.info('Creating composite index: idx_orders_tenant_created on orders(tenant_id, created_at DESC)...');
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_orders_tenant_created 
    ON orders (tenant_id, created_at DESC);
  `);
  logger.info('Composite index created successfully.');
}

/**
 * Drops the composite index (used to benchmark unindexed vs indexed performance)
 */
async function dropCompositeIndex() {
  const pool = getDatabasePool();
  logger.info('Dropping composite index: idx_orders_tenant_created...');
  await pool.query(`DROP INDEX IF EXISTS idx_orders_tenant_created;`);
  logger.info('Composite index dropped.');
}

module.exports = {
  runMigrations,
  createCompositeIndex,
  dropCompositeIndex
};
