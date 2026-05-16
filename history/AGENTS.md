# AGENTS.md (RETrend)

These instructions are for agentic coding tools working in this repo.
This project is in-progress and contains both active code and historical/backup code.

## Repository Map (What’s “active” vs “history”)
- `dags/`: Airflow DAGs (KubernetesPodOperator) that run crawler/backfill containers.
  - `dags/retrend_crawler_with_quota_dag.py`: daily KREB backfill (quota-aware) runner.
  - `dags/sample/`: older/sample DAGs; some referenced scripts may not exist anymore.
- `src/kreb/`: KREB ingestion/backfill project.
- `src/kreb/src/kreb_etl_v2/backfill.py`: current backfill engine (quota + cursor state + S3/MinIO writes).
- `docker/kreb-backfill/Dockerfile`: image for running the backfill container.
- `docker/crawler/Dockerfile`: general crawler image (copies `src/` to `/app/src`).
- `helm/`: Helm charts/values for Airflow and ingress.
- `commands.md`: operational commands (buildx, helm, kubectl) + smoke-test snippets.
- `docs/history/`: historical code snapshots and experiments; treat as read-only reference.

Notes:
- Some docs/DAGs reference scripts like `extract_shido_*` that currently live under `docs/history/` or are absent.
  Verify file existence before wiring pipelines to them.
- Data/output directories are mostly ignored by git; do not add new large artifacts.

## Build / Run / Test (Copy-paste)

### Python environment
- Python version: Docker images use Python 3.9; keep runtime-compatible syntax in active code.
- Create venv:
  - `python3 -m venv .venv && source .venv/bin/activate`

### Install KREB subproject (for local runs/tests)
- `pip install -e "src/kreb[dev]"`

### Run KREB backfill locally (file output)
Set required env vars (do NOT commit real keys):
- `export KREB_SERVICE_KEY=...`
- `export KREB_DAILY_LIMIT=10000`
- `export KREB_LAWD_CSV=/path/to/lawd.csv`
- `export KREB_STATE_URI=file:///tmp/kreb_state.json`
- `export KREB_OUTPUT_URI=file:///tmp/kreb_output`
- `python src/kreb/src/kreb_etl_v2/backfill.py`

### Airflow / Kubernetes (operational)
See `commands.md` for the full runbook. Key commands:
- Build crawler image: `docker buildx build --platform linux/amd64 -t dave126/retrend-crawler:<tag> -f docker/crawler/Dockerfile . --push`
- Deploy Airflow: `helm upgrade --install airflow apache-airflow/airflow -n airflow --create-namespace -f helm/airflow/airflow-onprem.yaml`
- Apply ingress: `kubectl apply -f helm/nginx/airflow-ingress-manual.yaml`

### Tests
There are tests under `src/kreb/tests/`.
- Run all tests (if pytest installed): `python -m pytest -q src/kreb/tests`
- Run a single test:
  - `python -m pytest -q src/kreb/tests/test_client.py::test_fetch_page_success`

Important: some tests currently import `kreb_etl.*` (a historical module under `docs/history/kreb_etl/`).
If tests fail due to missing modules or Python version syntax, fix the packaging/tests before relying on them.

### Lint / Format
- No repo-wide lint/format tool is configured.
- Do not introduce repo-wide formatting or new linters without an explicit request.
- If you add tooling, prefer `ruff` + `black` + `pytest` and document exact commands here.

## Coding Standards

### General
- Keep changes minimal and scoped; avoid “cleanup refactors” while fixing bugs.
- Prefer configuration/env vars over hard-coded paths and constants.
- Preserve historical code in `docs/history/` unless explicitly asked to prune it.

### Python style
- Indentation: 4 spaces; follow PEP 8.
- Naming:
  - modules/files/functions: `snake_case`
  - classes: `CapWords`
  - constants: `UPPER_SNAKE_CASE`
- Imports:
  - standard library, third-party, local; separated by blank lines.
  - no wildcard imports.
- Logging:
  - use `logging` (not `print`) for pipeline code.
  - include structured context in messages (lawd_cd, deal_ym, page, remaining quota).

### Types
- Active code should remain compatible with Python 3.9.
  - Avoid `X | None` union syntax (3.10+). Use `Optional[X]`.
- Add type hints for new/modified functions where practical.
- Avoid `Any` unless it is truly unavoidable (e.g., XML-to-dict parsing boundary).

### Error handling
- Catch narrowly; never swallow exceptions silently.
- For external APIs:
  - treat 429 (rate-limit/quota) as a first-class outcome.
  - retry transient failures (timeouts, 5xx) with bounded retries + backoff.
  - log the request context (endpoint, params, page) on failure.

## Data Pipeline Reliability Rules (KREB ingestion)

Constraints:
- The KREB API is quota-limited (roughly 10k requests/day). Design for resumability.

Guidelines:
- Idempotency: writing the same partition/page twice must not corrupt results.
- Checkpointing:
  - persist cursor/state to `KREB_STATE_URI` (S3/MinIO or local).
  - state updates should be atomic (write temp then move/rename).
- Completeness:
  - do not mark a job “done” unless you have actually finished the final (lawd_cd, deal_ym) pages.
  - prefer explicit completion signals (e.g., last page reached) over heuristics.
- Observability:
  - log per-page progress and a run summary (counts, remaining quota, last cursor).
  - when running in Airflow, rely on task logs for debugging; keep logs concise.

## Security / Secrets (Hard rule)

Do not commit secrets.
- This repo currently contains hard-coded credentials in a few places (DAGs/Dockerfiles/docs).
  When touching those files, move secrets to:
  - Airflow Variables/Connections/Secrets, or
  - Kubernetes Secrets mounted as env vars.
- Never print secrets in logs.

## Repo Hygiene

- Large artifacts: do not commit datasets (`data/`, `parquet/`, `trade_history/`, `tmp/`, `output/`).
- Respect `.gitignore` patterns; keep new outputs in ignored dirs.
- If operational steps change, update `commands.md`.

## Cursor/Copilot Rules

- No Cursor rules found in `.cursor/rules/` or `.cursorrules`.
- No Copilot instructions found in `.github/copilot-instructions.md`.
