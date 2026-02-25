---
layout: default
title: Home
---

# Platform Documentation Hub

<section class="hero">
  <p>
    Interactive docs for the retail analytics lakehouse. Use this site for architecture context,
    script-level API references, onboarding quickstart steps, and live demo access.
  </p>
  <p>
    Source code, pipelines, and infrastructure remain in the main repository:
    <a href="{{ site.repository_url }}">{{ site.repository_url }}</a>
  </p>
</section>

## Start Here

<section class="grid grid-3">
  <article class="card">
    <h3>Architecture</h3>
    <p>Open the Draw.io-backed architecture diagram and review key layer responsibilities.</p>
    <a class="btn" href="{{ '/architecture/' | relative_url }}">View Architecture</a>
  </article>
  <article class="card">
    <h3>Script API</h3>
    <p>Review command-line contracts for CI/data quality/benchmarking scripts.</p>
    <a class="btn" href="{{ '/api-reference/' | relative_url }}">View Script API</a>
  </article>
  <article class="card">
    <h3>Quickstart</h3>
    <p>Follow a practical setup flow with screenshots for local runs and validation checks.</p>
    <a class="btn" href="{{ '/quickstart/' | relative_url }}">Open Quickstart</a>
  </article>
</section>

## Live Demo

<section class="card">
  <p>Launch a deployed sample environment when available.</p>
  <p>
    <a class="btn" data-live-demo-link href="#">AWS Free Tier Demo</a>
  </p>
  <p class="status-note" data-live-demo-status></p>
</section>

## Related Repository Docs

- [Architecture deep dive]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/docs/architecture.md)
- [CI/CD quality gates]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/docs/ci-cd.md)
- [Federated querying guide]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/docs/federated-querying.md)
- [Performance benchmarks]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/perf/README.md)
