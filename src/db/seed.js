const { v4: uuidv4 } = require('uuid');
const { getDatabasePool } = require('../config/database');
const logger = require('../utils/logger');

const SEED_TENANTS = [
  { id: 'tenant-alpha', name: 'Alpha Logistics Corp', slug: 'alpha', subdomain: 'alpha' },
  { id: 'tenant-beta', name: 'Beta Retailers Ltd', slug: 'beta', subdomain: 'beta' },
  { id: 'tenant-gamma', name: 'Gamma Tech Global', slug: 'gamma', subdomain: 'gamma' }
];

const SEED_PRODUCTS = [
  // Tenant Alpha Products
  { id: 'prod_a1', tenant_id: 'tenant-alpha', sku: 'ALPH-SRV-01', name: 'Enterprise Cloud Server', price: 499.00, stock: 150 },
  { id: 'prod_a2', tenant_id: 'tenant-alpha', sku: 'ALPH-SWT-02', name: 'Gigabit Managed Switch', price: 129.50, stock: 320 },
  { id: 'prod_a3', tenant_id: 'tenant-alpha', sku: 'ALPH-CAB-03', name: 'Cat6 Shielded Cable (100m)', price: 34.00, stock: 800 },
  
  // Tenant Beta Products
  { id: 'prod_b1', tenant_id: 'tenant-beta', sku: 'BETA-LAP-01', name: 'Pro Ultrabook 15-inch', price: 1249.99, stock: 95 },
  { id: 'prod_b2', tenant_id: 'tenant-beta', sku: 'BETA-MOU-02', name: 'Ergonomic Wireless Mouse', price: 45.00, stock: 450 },
  { id: 'prod_b3', tenant_id: 'tenant-beta', sku: 'BETA-KEY-03', name: 'Mechanical RGB Keyboard', price: 89.90, stock: 210 },

  // Tenant Gamma Products
  { id: 'prod_g1', tenant_id: 'tenant-gamma', sku: 'GAM-MON-01', name: '4K Ultra-Wide Monitor', price: 699.00, stock: 60 },
  { id: 'prod_g2', tenant_id: 'tenant-gamma', sku: 'GAM-AUD-02', name: 'Noise-Cancelling Headset', price: 199.00, stock: 180 }
];

const SEED_USERS = [
  { id: 'usr_a1', tenant_id: 'tenant-alpha', email: 'alice@alpha.corp', name: 'Alice Alpha', role: 'ADMIN' },
  { id: 'usr_b1', tenant_id: 'tenant-beta', email: 'bob@beta.retail', name: 'Bob Beta', role: 'ADMIN' },
  { id: 'usr_g1', tenant_id: 'tenant-gamma', email: 'carol@gamma.tech', name: 'Carol Gamma', role: 'ADMIN' }
];

async function seedDatabase(initialOrderCount = 50) {
  const pool = getDatabasePool();
  logger.info('Starting database seeding...');

  try {
    // 1. Seed Tenants
    for (const t of SEED_TENANTS) {
      await pool.query(`
        INSERT INTO tenants (id, name, slug, subdomain, status)
        VALUES ($1, $2, $3, $4, 'ACTIVE')
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, subdomain = EXCLUDED.subdomain;
      `, [t.id, t.name, t.slug, t.subdomain]);
    }

    // 2. Seed Users
    for (const u of SEED_USERS) {
      await pool.query(`
        INSERT INTO users (id, tenant_id, email, name, role)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;
      `, [u.id, u.tenant_id, u.email, u.name, u.role]);
    }

    // 3. Seed Products
    for (const p of SEED_PRODUCTS) {
      await pool.query(`
        INSERT INTO products (id, tenant_id, sku, name, price, stock)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock;
      `, [p.id, p.tenant_id, p.sku, p.name, p.price, p.stock]);
    }

    // 4. Seed Orders & OrderItems for Tenant Alpha & Beta (for N+1 benchmarks and isolation testing)
    const existingOrders = await pool.query(`SELECT COUNT(*) as count FROM orders WHERE tenant_id = 'tenant-alpha'`);
    const currentCount = parseInt(existingOrders.rows[0]?.count || '0', 10);

    if (currentCount < initialOrderCount) {
      const ordersToInsert = initialOrderCount - currentCount;
      logger.info(`Seeding ${ordersToInsert} orders with items for performance benchmarks...`);

      for (let i = 0; i < ordersToInsert; i++) {
        const orderId = `ord_seed_${i + 1}`;
        const tenantId = (i % 2 === 0) ? 'tenant-alpha' : 'tenant-beta';
        const userId = (tenantId === 'tenant-alpha') ? 'usr_a1' : 'usr_b1';
        const prod = (tenantId === 'tenant-alpha') ? SEED_PRODUCTS[0] : SEED_PRODUCTS[3];
        const qty = (i % 3) + 1;
        const total = (Number(prod.price) * qty).toFixed(2);
        
        // Stagger timestamps across the past 60 days
        const pastDate = new Date(Date.now() - (i * 3600 * 1000 * 8)).toISOString();

        await pool.query(`
          INSERT INTO orders (id, tenant_id, user_id, status, total_amount, created_at)
          VALUES ($1, $2, $3, 'CONFIRMED', $4, $5)
          ON CONFLICT (id) DO NOTHING;
        `, [orderId, tenantId, userId, total, pastDate]);

        await pool.query(`
          INSERT INTO order_items (id, tenant_id, order_id, product_id, quantity, unit_price, subtotal)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (id) DO NOTHING;
        `, [`item_seed_${i + 1}_1`, tenantId, orderId, prod.id, qty, prod.price, total]);
      }
    }

    logger.info('Database seeding completed successfully.');
    return true;
  } catch (err) {
    logger.error('Database seeding failed', { error: err.message });
    throw err;
  }
}

module.exports = {
  seedDatabase,
  SEED_TENANTS,
  SEED_PRODUCTS,
  SEED_USERS
};
