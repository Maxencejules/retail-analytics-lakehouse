---
layout: default
title: Architecture
---

# Architecture Diagram

This page exposes an editable Draw.io source and an interactive viewer for the platform topology.

## Interactive Draw.io View

<section class="card">
  <p>
    <a id="drawio-viewer-link" class="btn" href="#" target="_blank" rel="noopener">
      Open Interactive Diagram
    </a>
    <a id="drawio-source-link" class="btn secondary" href="#" target="_blank" rel="noopener">
      Open Raw .drawio Source
    </a>
  </p>
  <p id="drawio-link-note" class="status-note">
    If links are unavailable, set <code>repository_url</code> in <code>docs-site/_config.yml</code>.
  </p>
</section>

<section class="diagram-wrap">
  <iframe
    id="drawio-embed"
    class="diagram-frame"
    title="Retail Lakehouse Architecture Diagram"
    loading="lazy"
  ></iframe>
</section>

## Layer Summary

- Sources and ingestion: POS/eCommerce/CRM streams via Kafka plus batch file/API feeds.
- Lakehouse layers: Bronze (raw), Silver (cleansed), Gold (analytics-ready marts).
- Warehouse serving: Postgres (local) and cloud warehouse targets for BI delivery.
- Federation and observability: Trino for cross-store SQL; monitoring and lineage for operations.

## Diagram Source Control

- Draw.io source file:
  [`docs-site/assets/diagrams/lakehouse-architecture.drawio`]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/docs-site/assets/diagrams/lakehouse-architecture.drawio)
- To update:
  1. Open the source in diagrams.net.
  2. Save updates back to the same `.drawio` file.
  3. Commit and push to trigger the Pages deployment workflow.
