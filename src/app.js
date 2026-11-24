const express = require('express');
const cors = require('cors');
const path = require('path');
const { tenantResolver } = require('./middleware/tenantResolver');
const errorHandler = require('./middleware/errorHandler');

// Route imports
const healthRoutes = require('./routes/healthRoutes');
const orderRoutes = require('./routes/orderRoutes');
const productRoutes = require('./routes/productRoutes');
const benchmarkRoutes = require('./routes/benchmarkRoutes');

const app = express();

const fs = require('fs');

// Global Middleware
app.use(cors());
app.use(express.json());

// Determine writable / static invoice storage directory
const isServerless = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
const invoiceDir = isServerless ? path.join('/tmp', 'invoices') : path.join(__dirname, '../storage/invoices');

// Serve Static Frontend Dashboard
app.use(express.static(path.join(__dirname, '../public')));
app.use('/invoices', express.static(invoiceDir));

// Preload dashboard HTML for bulletproof serverless delivery
let cachedDashboardHtml = '';
try {
  const htmlPath = path.join(__dirname, '../public/index.html');
  if (fs.existsSync(htmlPath)) {
    cachedDashboardHtml = fs.readFileSync(htmlPath, 'utf-8');
  }
} catch (e) {
  // Ignored
}

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    if (process.env.NODE_ENV !== 'test') {
      console.log(`[HTTP] ${req.method} ${req.originalUrl} ${res.statusCode} (${duration}ms) [Tenant: ${req.tenantId || 'NONE'}]`);
    }
  });
  next();
});

// Root Information Endpoint (Serves Dashboard on browser / HTML, JSON for API clients)
app.get('/', (req, res) => {
  if (req.accepts('html') && cachedDashboardHtml) {
    return res.type('html').send(cachedDashboardHtml);
  }
  const indexPath = path.join(__dirname, '../public/index.html');
  if (fs.existsSync(indexPath)) {
    return res.sendFile(indexPath);
  }
  res.json({
    name: 'Multi-Tenant SaaS E-Commerce Platform',
    version: '1.0.0',
    documentation: 'See README.md for full architecture and lecture requirement breakdown.',
    endpoints: {
      health: '/api/v1/health',
      connectionPoolStatus: '/api/v1/health/pool',
      kafkaConsumerStatus: '/api/v1/health/kafka',
      orders: '/api/v1/orders',
      products: '/api/v1/products',
      nPlusOneBenchmark: '/api/v1/benchmarks/n-plus-one',
      explainAnalyzeBenchmark: '/api/v1/benchmarks/explain-analyze'
    }
  });
});

// Public Health Check Routes (Exempt from Tenant Resolution)
app.use('/api/v1/health', healthRoutes);

// Apply Tenant Resolution Middleware to All Tenant-Scoped API Routes (Lecture 18)
app.use('/api/v1', tenantResolver);

// Tenant-Scoped API Routes
app.use('/api/v1/orders', orderRoutes);
app.use('/api/v1/products', productRoutes);
app.use('/api/v1/benchmarks', benchmarkRoutes);

// Global Error Handler
app.use(errorHandler);

module.exports = app;
