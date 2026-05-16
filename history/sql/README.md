# SQL

Trino/BI에서 사용하는 SQL(DDL/뷰/쿼리) 레이어입니다.

이 디렉터리를 **단일 SQL 소스**로 사용합니다.

## Superset dataset SQL

- 경로: `sql/superset/datasets/*.sql`
- 파일명 규칙: `<superset_dataset_table_name>.sql`
- placeholder:
  - `{{SGG_MAP_CTE}}`: 시군구 한글명 고정 매핑 CTE (sync 스크립트에서 생성/주입)
  - `{{SQL_<DATASET_TABLE_NAME_UPPER>}}`: 다른 dataset SQL 참조

## Sync

`infra/superset/sync_superset_sql.py`가 위 SQL 파일을 읽어 Superset dataset SQL을 create/update 합니다.

```bash
python infra/superset/sync_superset_sql.py
```
