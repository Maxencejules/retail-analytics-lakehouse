# GitHub Pages Docs Site

This folder contains a Jekyll-based documentation site deployed by GitHub Pages.

## Deploy Model

- Workflow: `.github/workflows/pages.yml`
- Source directory: `docs-site/`
- Deployment target: GitHub Pages environment (`github-pages`)

## Configure for Your Repository

Update the following values in `docs-site/_config.yml`:

- `repository_url`
- `docs_default_branch` (usually `main` or `master`)
- `live_demo_url` (optional AWS-hosted demo endpoint)

## Local Preview (Optional)

If you use Ruby locally:

```bash
bundle exec jekyll serve --source docs-site
```

If you prefer containerized preview:

```bash
docker run --rm -it -p 4000:4000 -v "$PWD:/srv/jekyll" jekyll/jekyll:pages \
  jekyll serve --source docs-site --watch --host 0.0.0.0
```
