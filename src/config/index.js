require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT || '4000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  apiPrefix: process.env.API_PREFIX || '/api/v1',
  jwtSecret: process.env.JWT_SECRET || 'super-secret-saas-tenant-key-2026',
  defaultTenantId: process.env.DEFAULT_TENANT_ID || 'tenant-alpha',
  
  db: {
    useEmbedded: process.env.DB_USE_EMBEDDED === 'true',
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432', 10),
    database: process.env.DB_NAME || 'saas_multitenant_db',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    ssl: process.env.DB_SSL === 'true',
    pool: {
      max: parseInt(process.env.DB_POOL_MAX || '20', 10),
      min: parseInt(process.env.DB_POOL_MIN || '4', 10),
      idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT_MS || '30000', 10),
      connectionTimeoutMillis: parseInt(process.env.DB_POOL_CONN_TIMEOUT_MS || '3000', 10),
      maxUses: 7500
    }
  },
  
  kafka: {
    enabled: process.env.KAFKA_ENABLED === 'true',
    clientId: process.env.KAFKA_CLIENT_ID || 'saas-order-service',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    topicOrders: process.env.KAFKA_TOPIC_ORDERS || 'tenant.orders',
    groupId: process.env.KAFKA_GROUP_ID || 'order-processing-group',
    ssl: process.env.KAFKA_SSL === 'true',
    sasl: process.env.KAFKA_SASL_USERNAME ? {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD
    } : undefined
  }
};
