# AGENTS.md

## Project Goals

- Build a modular retail analytics lakehouse for ingestion, transformation, warehousing, and BI.
- Keep components decoupled so teams can evolve each layer independently.
- Maintain reproducible local workflows that map cleanly to CI/CD.

## Conventions

- Runtime: Python 3.11.
- Formatting: `black`.
- Linting: `ruff`.
- Before opening PRs, run `make format`, `make lint`, and `make test`.
- Keep code and pipeline logic small, testable, and environment-configurable.

## Security Rule

- Never commit secrets (API keys, passwords, tokens, private certificates, or `.env` secrets).
- Use environment variables and a secret manager for sensitive values.

