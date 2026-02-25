---
layout: default
title: Live Demo
---

# Live Demo

Use this page to expose a deployed sample instance (for example on AWS Free Tier).

<section class="card">
  <p>
    <a class="btn" data-live-demo-link href="#">AWS Free Tier Demo</a>
  </p>
  <p class="status-note" data-live-demo-status></p>
</section>

## Configure Demo URL

Set the live demo URL in:

- `docs-site/_config.yml` via `live_demo_url`

Then push to your default branch to redeploy Pages.

## Temporary Override (No Commit Needed)

For previews or incident drills, append `?demo=<url>` to this docs page URL.

Example:

```text
https://<org>.github.io/<repo>/live-demo/?demo=https://<your-ec2-or-lightsail-host>
```

The site stores this value in browser local storage and uses it until cleared.

## AWS Free Tier Deployment Notes

- Host a sample dashboard/API on a `t2.micro`/`t3.micro` instance.
- Restrict inbound traffic with a security group and TLS.
- Keep sample data non-sensitive; never expose secrets in demo payloads.
