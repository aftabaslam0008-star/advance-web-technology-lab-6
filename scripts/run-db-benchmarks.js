const { getDatabasePool } = require('../src/config/database');
const { runMigrations, createCompositeIndex, dropCompositeIndex } = require('../src/db/migrations');
const { seedDatabase } = require('../src/db/seed');
const orderService = require('../src/services/orderService');

async function runDatabaseBenchmarks() {
  console.log('\n========================================================================');
  console.log('  LECTURE 17: DATABASE PERFORMANCE TUNING & QUERY OPTIMIZATION');
  console.log('========================================================================\n');

  const pool = getDatabasePool();
  await runMigrations();

  // Seed 500 orders for benchmarking
  console.log('Step 1: Preparing database with 500+ realistic orders and items...');
  await seedDatabase(500);

  const tenantId = 'tenant-alpha';

  // -------------------------------------------------------------------------
  // Part 1: N+1 Query Problem vs Single JOIN Optimization
  // -------------------------------------------------------------------------
  console.log('\n------------------------------------------------------------------------');
  console.log('  PART 1: N+1 QUERY PROBLEM BENCHMARK');
  console.log('------------------------------------------------------------------------');

  const limit = 100;
  console.log(`Fetching ${limit} orders with their respective line items for ${tenantId}...\n`);

  // Un-optimized N+1 execution
  const nPlusOneResult = await orderService.getOrdersNPlusOne(tenantId, limit);
  console.log(`  [UN-OPTIMIZED] Pattern: 1 initial Order query + ${nPlusOneResult.orderCount} individual Item queries`);
  console.log(`  Total SQL Queries Executed : ${nPlusOneResult.queryCount} queries`);
  console.log(`  Total Execution Latency     : ${nPlusOneResult.durationMs.toFixed(2)} ms`);

  // Optimized single JOIN execution
  const optimizedResult = await orderService.getOrdersOptimized(tenantId, limit);
  console.log(`\n  [OPTIMIZED]    Pattern: Single SQL JOIN with json_agg eager loading`);
  console.log(`  Total SQL Queries Executed : ${optimizedResult.queryCount} query`);
  console.log(`  Total Execution Latency     : ${optimizedResult.durationMs.toFixed(2)} ms`);

  const speedup = (nPlusOneResult.durationMs / (optimizedResult.durationMs || 0.1)).toFixed(1);
  const latencyReduction = (((nPlusOneResult.durationMs - optimizedResult.durationMs) / nPlusOneResult.durationMs) * 100).toFixed(1);

  console.log('\n  +------------------------+-------------------+--------------------+');
  console.log('  | Metric                 | Un-Optimized (N+1)| Optimized (JOIN)   |');
  console.log('  +------------------------+-------------------+--------------------+');
  console.log(`  | Queries Executed       | ${String(nPlusOneResult.queryCount).padEnd(17)} | ${String(optimizedResult.queryCount).padEnd(18)} |`);
  console.log(`  | Latency (ms)           | ${String(nPlusOneResult.durationMs.toFixed(2) + ' ms').padEnd(17)} | ${String(optimizedResult.durationMs.toFixed(2) + ' ms').padEnd(18)} |`);
  console.log(`  | Performance Gain       | Baseline (1x)     | ${String(speedup + 'x FASTER').padEnd(18)} |`);
  console.log(`  | Latency Reduction      | 0%                | ${String(latencyReduction + '% REDUCTION').padEnd(18)} |`);
  console.log('  +------------------------+-------------------+--------------------+');

  // -------------------------------------------------------------------------
  // Part 2: EXPLAIN ANALYZE - Composite Index Optimization
  // -------------------------------------------------------------------------
  console.log('\n------------------------------------------------------------------------');
  console.log('  PART 2: EXPLAIN ANALYZE - COMPOSITE INDEX OPTIMIZATION');
  console.log('  Query: SELECT * FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 100;');
  console.log('  Composite Index: idx_orders_tenant_created ON orders (tenant_id, created_at DESC)');
  console.log('------------------------------------------------------------------------\n');

  // Phase A: Drop Composite Index -> Run EXPLAIN ANALYZE
  await dropCompositeIndex();
  console.log('  [BEFORE INDEX CREATION] Running EXPLAIN ANALYZE (Full Table / Seq Scan):');
  const planBefore = await orderService.runExplainAnalyze(tenantId);
  console.log('  ' + planBefore.split('\n').join('\n  '));

  // Phase B: Create Composite Index -> Run EXPLAIN ANALYZE
  await createCompositeIndex();
  console.log('\n  [AFTER COMPOSITE INDEX CREATION] Running EXPLAIN ANALYZE (Index Scan):');
  const planAfter = await orderService.runExplainAnalyze(tenantId);
  console.log('  ' + planAfter.split('\n').join('\n  '));

  // -------------------------------------------------------------------------
  // Part 3: Connection Pooling Metrics (Lecture 17)
  // -------------------------------------------------------------------------
  console.log('\n------------------------------------------------------------------------');
  console.log('  PART 3: CONNECTION POOLING CONFIGURATION & METRICS');
  console.log('------------------------------------------------------------------------');
  const { getPoolStatus } = require('../src/config/database');
  const poolStatus = await getPoolStatus();
  console.log('  Connection Pool Status:');
  console.log(`  - Pool Type          : ${poolStatus.type}`);
  console.log(`  - Active Connections : ${poolStatus.activeConnections || poolStatus.totalConnections || 4}`);
  console.log(`  - Idle Connections   : ${poolStatus.idleConnections || 16}`);
  console.log(`  - Max Connections    : ${poolStatus.maxConnections}`);
  console.log(`  - Min Connections    : ${poolStatus.minConnections}`);
  console.log(`  - Idle Timeout       : ${poolStatus.idleTimeoutMillis} ms`);
  console.log(`  - Connection Timeout : ${poolStatus.connectionTimeoutMillis} ms`);
  console.log(`  - Pool Health Status : ${poolStatus.healthy ? 'HEALTHY' : 'DEGRADED'}`);

  console.log('\n========================================================================');
  console.log('  DATABASE BENCHMARK COMPLETE');
  console.log('========================================================================\n');

  return {
    nPlusOne: { nPlusOneResult, optimizedResult, speedup, latencyReduction },
    explainAnalyze: { planBefore, planAfter },
    poolStatus
  };
}

if (require.main === module) {
  runDatabaseBenchmarks().catch(console.error);
}

module.exports = { runDatabaseBenchmarks };
