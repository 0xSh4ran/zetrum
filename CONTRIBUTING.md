# Contributing to Zetrum

Thank you for your interest in contributing to Zetrum! This document outlines the process for contributing code, documentation, and ideas.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Branch Naming](#branch-naming)
- [Commit Messages](#commit-messages)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)

---

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

---

## How to Contribute

There are many ways to contribute:

- **Report bugs** — Open an issue describing the bug, steps to reproduce, and expected vs actual behavior
- **Suggest features** — Open an issue with the `enhancement` label describing your idea
- **Fix bugs** — Pick up an open issue labeled `bug` and submit a pull request
- **Write documentation** — Improve runbooks, architecture docs, or inline code comments
- **Write tests** — Add unit or integration tests under `ci/tests/`
- **Build pipeline layers** — Contribute Spark jobs, Airflow DAGs, or Great Expectations suites

---

## Development Setup

### Prerequisites

- Ubuntu 22.04+ or any Linux distro
- Docker + Docker Compose v2
- Python 3.10+
- Git
- 8GB RAM minimum

### Steps

```bash
# 1. Fork the repo on GitHub, then clone your fork
git clone https://github.com/<your-username>/zetrum.git
cd zetrum

# 2. Add upstream remote
git remote add upstream https://github.com/0xSh4ran/zetrum.git

# 3. Start the local infrastructure
docker compose -f infra/docker-compose.yml up -d

# 4. Install Python dependencies
pip install confluent-kafka==2.3.0 fastavro faker httpx authlib cachetools --user

# 5. Verify the producer works
python3 ingestion/kafka/producers/clickstream_producer.py --events 10
```

---

## Branch Naming

Use descriptive branch names following this convention:

| Type | Pattern | Example |
|---|---|---|
| Feature | `feat/<description>` | `feat/bronze-spark-consumer` |
| Bug fix | `fix/<description>` | `fix/kafka-healthcheck` |
| Documentation | `docs/<description>` | `docs/architecture-diagram` |
| Refactor | `refactor/<description>` | `refactor/producer-error-handling` |
| CI/CD | `ci/<description>` | `ci/add-unit-tests` |

---

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>: <short description>

[optional body]
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `ci`, `chore`

Examples:
```
feat: add Spark Structured Streaming Bronze consumer
fix: resolve Zookeeper healthcheck failure on first startup
docs: add Bronze layer runbook
refactor: extract event generator into separate class
```

---

## Pull Request Process

1. Create a branch from `main` using the naming convention above
2. Make your changes with clear, focused commits
3. Ensure your code follows the coding standards below
4. Update relevant documentation (README, runbooks, CHANGELOG)
5. Open a pull request against `main` with:
   - A clear title following commit message conventions
   - A description of what changed and why
   - Steps to test the change locally
6. Address any review comments

---

## Coding Standards

### Python

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints where possible
- Write docstrings for all classes and public methods
- Keep functions small and focused — one responsibility per function
- Handle exceptions explicitly, never use bare `except:`

### Docker / Infrastructure

- Always set memory limits on containers
- Use named volumes, not bind mounts for persistent data
- Pin image versions — never use `latest` in production configs
- Document all environment variables with comments

### Documentation

- Update `CHANGELOG.md` with every meaningful change
- Keep runbooks in `docs/runbooks/` up to date
- Architecture decisions go in `docs/design/decisions.md`

---

## Questions?

Open a GitHub Discussion or file an issue with the `question` label.
