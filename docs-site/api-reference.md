---
layout: default
title: Script API Reference
---

# Script API Reference

Use this page as the command contract for key custom scripts used in CI/CD and local operations.

<p>
  <label for="api-filter"><strong>Filter scripts</strong></label><br>
  <input id="api-filter" class="input" type="text" placeholder="Type script name, flag, or domain (dbt/soda/benchmark)">
</p>

<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th>Script</th>
        <th>Purpose</th>
        <th>Common Flags</th>
        <th>Example</th>
      </tr>
    </thead>
    <tbody>
      <tr data-api-row data-search="benchmark etl runtime memory throughput rows iterations">
        <td><code>scripts/benchmark_etl.py</code></td>
        <td>Benchmarks generator + Spark batch ETL runtime, peak RSS memory, and throughput.</td>
        <td><code>--rows</code>, <code>--iterations</code>, <code>--warmup-iterations</code>, <code>--output</code></td>
        <td><code>python scripts/benchmark_etl.py --rows 5000 --iterations 3</code></td>
      </tr>
      <tr data-api-row data-search="integration test spark gold rows fail fast">
        <td><code>scripts/ci_integration_test.py</code></td>
        <td>End-to-end generator + ETL + Gold validation, including a fail-fast negative test.</td>
        <td><code>--rows</code></td>
        <td><code>python scripts/ci_integration_test.py --rows 1000</code></td>
      </tr>
      <tr data-api-row data-search="dbt slim ci state modified profiles selector">
        <td><code>scripts/dbt_slim_ci.py</code></td>
        <td>Builds dbt state and resolves slim selection for modified models in CI.</td>
        <td><code>--project-dir</code>, <code>--profiles-dir</code>, <code>--state-dir</code>, <code>--baseline-ref</code></td>
        <td><code>python scripts/dbt_slim_ci.py --target dev</code></td>
      </tr>
      <tr data-api-row data-search="soda quality checks alerts webhook sns target">
        <td><code>scripts/run_soda_scan.py</code></td>
        <td>Runs Soda checks and routes failures to webhook/SNS integrations.</td>
        <td><code>--configuration</code>, <code>--checks</code>, <code>--target</code>, <code>--data-source</code></td>
        <td><code>python scripts/run_soda_scan.py --checks quality/soda/checks/gold_quality.yml</code></td>
      </tr>
      <tr data-api-row data-search="dbt governance semantic exposures contracts validation">
        <td><code>scripts/validate_dbt_governance.py</code></td>
        <td>Validates dbt semantic/governance contracts for owned, tagged, exposed assets.</td>
        <td><code>--repo-root</code></td>
        <td><code>python scripts/validate_dbt_governance.py --repo-root .</code></td>
      </tr>
      <tr data-api-row data-search="phase3 policy spark redshift s3 budget log retention">
        <td><code>scripts/validate_phase3_policies.py</code></td>
        <td>Validates Phase 3 policy JSON artifacts for performance and cost controls.</td>
        <td><code>--repo-root</code></td>
        <td><code>python scripts/validate_phase3_policies.py --repo-root .</code></td>
      </tr>
    </tbody>
  </table>
</div>

<p id="api-filter-empty" class="status-note warn" style="display:none;">
  No scripts matched the current filter.
</p>

## Recommended Entry Points

- Local quality and tests: `make lint`, `make test`, `make benchmark-etl`
- Operational checks: `make dbt-governance-validate`, `make phase3-policy-validate`, `make soda-scan`
- CI implementation: `.github/workflows/ci.yml`
