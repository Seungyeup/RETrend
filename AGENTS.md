# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Python data pipelines and apps (extract scripts, `phase1–4`, Dockerfiles, K8s manifests).
- `dags/`: Airflow DAGs (e.g., `retrend_crawler_dag.py`).
- `helm/`: Helm charts and values for Airflow, nginx, metallb.
- `data/`, `parquet/`, `trade_history/`: large datasets; do not commit new large binaries.
- `tmp/`, `output/`, `sample/`: scratch artifacts, generated outputs, examples.
- `commands.md`: frequently used Docker/Helm/Kubernetes commands.

## Build, Test, and Development Commands
- Local script run: `python src/extract_shido_to_excel.py` (see other `src/extract_*` scripts).
- Streamlit app: `streamlit run src/phase2/streamlit_app.py`.
- Build crawler image: `docker buildx build --platform linux/amd64 -t dave126/retrend-crawler:2.8.4 -f src/docker/crawler/Dockerfile . --push`.
- Deploy Airflow (Helm): `helm upgrade --install airflow apache-airflow/airflow -n airflow -f helm/airflow/airflow-onprem.yaml`.
- Ingress example: `kubectl apply -f helm/nginx/airflow-ingress-manual.yaml`.

## Coding Style & Naming Conventions
- Python: PEP 8, 4‑space indents, `snake_case` for files/functions, `CapWords` for classes.
- YAML/K8s/Helm: two‑space indents; use `kebab-case` for resource names; keep values in `helm/airflow/airflow-onprem.yaml`.
- Images/tags follow existing pattern: `dave126/retrend-crawler:<version>`.

## Testing Guidelines
- No formal suite yet. Prefer pure functions and small modules.
- If adding tests, use `pytest`, place under `tests/`, name `test_*.py`.
- For data code, add quick smoke runs (small samples in `sample/`) and validate schema/row counts.

## Commit & Pull Request Guidelines
- Use clear, scoped commits. Prefer Conventional Commit prefixes when possible: `feat:`, `fix:`, `chore:`, `docs:`.
- PRs include: summary, rationale, reproduction steps/commands, screenshots for UI (Streamlit) or logs for jobs, and any Helm/K8s changes.
- Do not commit secrets or large data. Update `commands.md` if operational steps change.

## Security & Configuration Tips
- Never commit credentials; use Kubernetes Secrets and environment variables. Review `helm/airflow/airflow-onprem.yaml` for sensitive fields (e.g., `fernetKey`).
- Make file paths configurable via env vars; avoid hard‑coded user home paths in new code.

## Agent Notes
- Keep changes minimal and focused; match existing patterns in `dags/` and `src/`.
- Prefer adding configuration over hardcoding; document new commands in `commands.md`.
