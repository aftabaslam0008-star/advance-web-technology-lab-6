const fs = require('fs');
const path = require('path');
const { getDatabasePool } = require('../config/database');
const logger = require('../utils/logger');

const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

async function runMigrations() {
  const pool = getDatabasePool();
  logger.info('Running database schema migrations...');

  try {
    const ddl = fs.readFileSync(SCHEMA_PATH, 'utf-8');
    
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
