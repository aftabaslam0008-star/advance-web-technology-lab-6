const express = require('express');
const router = express.Router();
const orderService = require('../services/orderService');
const { createCompositeIndex, dropCompositeIndex } = require('../db/migrations');

/**
 * GET /api/v1/benchmarks/n-plus-one
 * Lecture 17 Requirement: Benchmark and Optimize Slow Queries
 * Executes and compares un-optimized N+1 query vs single JOIN optimized query
 */
router.get('/n-plus-one', async (req, res, next) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const tenantId = req.tenantId;

    // 1. Run un-optimized N+1 query pattern
    const nPlusOneResult = await orderService.getOrdersNPlusOne(tenantId, limit);

    // 2. Run optimized single JOIN query pattern
    const optimizedResult = await orderService.getOrdersOptimized(tenantId, limit);

    const speedupMultiplier = (nPlusOneResult.durationMs / (optimizedResult.durationMs || 0.1)).toFixed(2);
    const latencyReductionPercent = (
      ((nPlusOneResult.durationMs - optimizedResult.durationMs) / nPlusOneResult.durationMs) * 100
    ).toFixed(2);

    res.json({
      benchmark: 'Lecture 17: N+1 Query Problem vs Single JOIN Optimization',
      tenantId,
      ordersEvaluated: nPlusOneResult.orderCount,
      comparison: {
        unoptimized: {
          strategy: 'N+1 Individual Queries (1 order query + N item queries)',
          queriesExecuted: nPlusOneResult.queryCount,
          executionTimeMs: nPlusOneResult.durationMs
        },
        optimized: {
          strategy: 'Single SQL JOIN with json_agg',
          queriesExecuted: optimizedResult.queryCount,
          executionTimeMs: optimizedResult.durationMs
        },
        performanceGain: {
          speedupMultiplier: `${speedupMultiplier}x faster`,
          latencyReduction: `${latencyReductionPercent}%`
        }
      }
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/v1/benchmarks/explain-analyze
 * Lecture 17 Requirement: Run EXPLAIN ANALYZE before and after composite indexing
 */
router.get('/explain-analyze', async (req, res, next) => {
  try {
    const tenantId = req.tenantId;

    // Phase 1: Drop index and run EXPLAIN ANALYZE (Unindexed / Seq Scan)
    await dropCompositeIndex();
    const planBefore = await orderService.runExplainAnalyze(tenantId);

    // Phase 2: Create composite index and run EXPLAIN ANALYZE (Indexed / Bitmap Scan)
    await createCompositeIndex();
    const planAfter = await orderService.runExplainAnalyze(tenantId);

    res.json({
      benchmark: 'Lecture 17: EXPLAIN ANALYZE Composite Index Optimization',
      targetQuery: "SELECT * FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 100",
      compositeIndex: "idx_orders_tenant_created ON orders(tenant_id, created_at DESC)",
      executionPlans: {
        beforeCompositeIndex: planBefore,
        afterCompositeIndex: planAfter
      }
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
