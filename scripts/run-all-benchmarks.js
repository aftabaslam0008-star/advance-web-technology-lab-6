const { runTenantIsolationTests } = require('./test-tenant-isolation');
const { runDatabaseBenchmarks } = require('./run-db-benchmarks');
const { runColdStartAnalysis } = require('./benchmark-cold-start');

async function main() {
  console.log('\n========================================================================');
  console.log('  LAB PROJECT 6: COMPLETE END-TO-END VERIFICATION & BENCHMARK HARNESS');
  console.log('========================================================================\n');

  try {
    // 1. Run Lecture 18 Tenant Isolation Tests
    await runTenantIsolationTests();

    // 2. Run Lecture 17 Database Performance Tuning & Optimization
    await runDatabaseBenchmarks();

    // 3. Run Lecture 17 Serverless Cold-Start Analysis
    await runColdStartAnalysis();

    console.log('\n========================================================================');
    console.log('  ALL ASSIGNMENT REQUIREMENTS VERIFIED SUCCESSFULLY WITH 100% PASS');
    console.log('========================================================================\n');
    process.exit(0);
  } catch (err) {
    console.error('Benchmark suite encountered an error:', err);
    process.exit(1);
  }
}

main();
