const app = require('../src/app');
const { runMigrations, createCompositeIndex } = require('../src/db/migrations');
const { seedDatabase } = require('../src/db/seed');
const { initProducer } = require('../src/kafka/producer');
const { startOrderConsumer } = require('../src/kafka/orderConsumer');

let isInitialized = false;
let initPromise = null;

/**
 * Lazy bootstrap function for Vercel Serverless environment
 */
async function ensureServerlessBootstrap() {
  if (isInitialized) return;
  if (!initPromise) {
    initPromise = (async () => {
      try {
        console.log('[VERCEL SERVERLESS] Bootstrapping schema migrations and initial state...');
        await runMigrations();
        await createCompositeIndex();
        await seedDatabase(20);
        await initProducer();
        await startOrderConsumer();
        isInitialized = true;
        console.log('[VERCEL SERVERLESS] Bootstrap complete.');
      } catch (err) {
        console.error('[VERCEL SERVERLESS] Bootstrap warning:', err.message);
        // Do not crash the entire function; let incoming requests still be handled
        isInitialized = true;
      }
    })();
  }
  return initPromise;
}

/**
 * Vercel Serverless Function Handler
 */
module.exports = async (req, res) => {
  try {
    await Promise.race([
      ensureServerlessBootstrap(),
      new Promise(resolve => setTimeout(resolve, 1500))
    ]);
  } catch (e) {
    // proceed
  }

  // Handle URL normalization if Vercel strips /api or rewrites it
  const matchedPath = req.headers['x-matched-path'];
  if (matchedPath && matchedPath.startsWith('/api') && !req.url.startsWith('/api')) {
    req.url = matchedPath;
  }

  return app(req, res);
};
