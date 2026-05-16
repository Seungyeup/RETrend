# Draft: RETrend 권장골격 정리 + 실행 가이드 문서

## Requirements (confirmed)
- 프로젝트를 권장골격(예: `pipelines/`, `jobs/`, `src/`, `infra/`, `sql/`, `docs/`, `tests/`)에 맞춰 폴더/파일을 재배치한다.
- 단일 문서(한국어) 1개로 레이어별 실행 단계/실행 커맨드/검증 방법을 정리한다.
- 마이그레이션은 "리스크 최소"가 목표이며, 기존 운영 경로를 깨지 않도록 shim/wrapper를 남긴다.

## Hard Constraints
- Backward compatibility (필수): 기존 실행 경로를 유지
  - Airflow: `dags/retrend_crawler_with_quota_dag.py` 가 실행하는 `python /app/src/kreb/src/kreb_etl_v2/backfill.py` 는 계속 동작해야 함 (경로 유지 또는 shim으로 보장).
- No commits unless asked.
- Runtime Python: 3.9 호환 유지.
- 외부 Airflow DAG 배포 repo/구성 변경은 가정하지 않음(이 repo 밖 변경을 전제로 하지 않음).

## Research Findings (repo scan)
- Airflow DAG(활성): `dags/retrend_crawler_with_quota_dag.py`
  - `image="dave126/kreb-backfill:0.1.1"`
  - `cmds=["python"]`, `arguments=["/app/src/kreb/src/kreb_etl_v2/backfill.py"]`
- Backfill 엔진(활성): `src/kreb/src/kreb_etl_v2/backfill.py` (quota/state/output 포함)
- Spark 적재(브론즈 -> Iceberg): `src/spark/kreb_csv_to_iceberg.py`, `src/spark/k8s/kreb_csv_to_iceberg.yaml`
- Dockerfile (kreb backfill 이미지): `src/kreb/src/docker/Dockerfile`
  - 현재 `COPY src /app/src` 이므로 컨테이너에 "repo root"가 아니라 "repo의 src/ 디렉토리"만 들어감.
- 관련 문서/커맨드:
  - `docs/runbook_kreb_backfill.md`
  - `src/kreb/src/kreb_etl_v2/commands.md`
  - `commands.md`
- Test infra 존재:
  - `src/kreb/pyproject.toml` 에 `pytest`(dev) 정의
  - `src/kreb/tests/` 에 테스트 존재 (일부는 `kreb_etl_v2.backfill` import)

## Open Questions
- "권장골격"의 기준/레퍼런스: 어떤 표준을 따라야 하는지(사내 템플릿/링크/예시 repo)가 있는지.
- 단일 문서의 범위: (1) 로컬 개발/스모크만, (2) k8s 운영까지, (3) Trino/Superset 쿼리/대시보드까지 포함 여부.
- 테스트 전략: 리팩터링에 대해 (TDD / 구현 후 테스트 추가 / 수동 검증 위주) 중 어느 쪽을 원하시는지.

## Scope Boundaries
- INCLUDE: 타겟 디렉토리 트리 제안, 구체적 이동/rename 절차, shim/wrapper 설계, 변경 파일 목록(최소), 검증 플랜(py_compile/pytest/수동), 단일 한국어 실행 문서.
- EXCLUDE: 이 repo 밖(배포된 Airflow gitSync repo 등) 변경을 전제로 한 작업.
