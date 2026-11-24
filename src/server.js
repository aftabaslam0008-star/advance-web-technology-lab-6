const app = require('./app');
const config = require('./config');
const { runMigrations, createCompositeIndex } = require('./db/migrations');
const { seedDatabase } = require('./db/seed');
const { initProducer } = require('./kafka/producer');
const { startOrderConsumer } = require('./kafka/orderConsumer');
const logger = require('./utils/logger');

async function bootstrap() {
  try {
    logger.info('====================================================');
    logger.info('Starting Multi-Tenant SaaS Backend Application...');
    logger.info('====================================================');

    // 1. Initialize Database Schema & Migrations
    await runMigrations();

    // 2. Ensure Composite Index is configured (Lecture 17)
    await createCompositeIndex();

    // 3. Seed Initial Tenants, Products, and Orders
    const seedOrderCount = (process.env.RENDER || process.env.VERCEL) ? 10 : 30;
    await seedDatabase(seedOrderCount);

    // 4. Initialize Kafka Producer & Start Kafka Consumer Group (Lecture 18)
    await initProducer();
    await startOrderConsumer();

    // 5. Start Express HTTP Server
    const server = app.listen(config.port, () => {
      logger.info(`Server successfully listening on http://localhost:${config.port}`);
      logger.info(`Health check: http://localhost:${config.port}/api/v1/health`);
      logger.info(`Connection Pool Metrics: http://localhost:${config.port}/api/v1/health/pool`);
      logger.info(`Kafka Consumer Metrics: http://localhost:${config.port}/api/v1/health/kafka`);
      logger.info('Multi-Tenant Context Discriminator: ACTIVE');
    });

    // Graceful Shutdown
    const shutdown = async (signal) => {
      logger.info(`Received ${signal}. Shutting down gracefully...`);
      server.close(() => {
        logger.info('HTTP server closed.');
        process.exit(0);
      });
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

  } catch (error) {
    logger.error('Failed to start application', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

if (require.main === module) {
  bootstrap();
}

module.exports = { bootstrap };
