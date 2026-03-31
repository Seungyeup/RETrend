from __future__ import annotations

import datetime as dt
import json
import os
import re
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests


BASE_URL = os.environ.get("SUPERSET_BASE_URL", "http://localhost:8088").rstrip("/")
USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")
DATABASE_NAME = os.environ.get("SUPERSET_DATABASE_NAME", "RETrend Trino Iceberg")

SQL_DIR = Path(os.environ.get("SUPERSET_SQL_DIR", "sql/superset/datasets")).resolve()

PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Z0-9_]+)\s*\}\}")
TABLE_REF_RE = re.compile(
    r"(?is)\b(?:from|join)\s+([`\"]?[A-Za-z_][A-Za-z0-9_]*[`\"]?)\.([`\"]?[A-Za-z_][A-Za-z0-9_]*[`\"]?)\.([`\"]?[A-Za-z_][A-Za-z0-9_]*[`\"]?)"
)


def env_to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes", "y", "on"):
        return True
    if normalized in ("0", "false", "no", "n", "off"):
        return False
    return default


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def normalize_identifier(token: str) -> str:
    raw = token.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ('"', "`"):
        return raw[1:-1]
    return raw


def extract_input_datasets(sql: str) -> List[Tuple[str, str]]:
    refs: Set[Tuple[str, str]] = set()
    for match in TABLE_REF_RE.finditer(sql):
        catalog = normalize_identifier(match.group(1))
        schema = normalize_identifier(match.group(2))
        table = normalize_identifier(match.group(3))
        refs.add((catalog, f"{schema}.{table}"))
    return sorted(refs)


def build_openlineage_endpoint(base_url: str, endpoint: str) -> str:
    clean_base = base_url.rstrip("/")
    if clean_base.endswith("/api/v1/lineage"):
        return clean_base
    return f"{clean_base}/{endpoint.strip('/')}"


class OpenLineageEmitter:
    def __init__(
        self,
        base_url: str,
        endpoint: str,
        namespace: str,
        job_prefix: str,
        output_namespace: str,
        output_name_prefix: str,
        input_namespace_override: Optional[str],
        skip_no_inputs: bool,
        enabled: bool,
        strict: bool,
    ) -> None:
        self.base_url = base_url.strip()
        self.endpoint = endpoint
        self.namespace = namespace
        self.job_prefix = job_prefix
        self.output_namespace = output_namespace
        self.output_name_prefix = output_name_prefix
        self.input_namespace_override = input_namespace_override
        self.skip_no_inputs = skip_no_inputs
        self.enabled = enabled and bool(self.base_url)
        self.strict = strict
        self.session = requests.Session()

    def emit_dataset_lineage(
        self,
        table_name: str,
        sql: str,
        input_datasets: List[Tuple[str, str]],
    ) -> None:
        if not self.enabled:
            return

        normalized_inputs = [
            (self.input_namespace_override or ns, name)
            for ns, name in input_datasets
        ]

        if not normalized_inputs and self.skip_no_inputs:
            message = (
                "Skipping OpenLineage emit because no input datasets were parsed "
                f"for table={table_name}"
            )
            if self.strict:
                raise RuntimeError(message)
            print(f"[lineage] WARN {message}")
            return

        event = {
            "eventType": "COMPLETE",
            "eventTime": now_iso(),
            "run": {
                "runId": str(uuid.uuid4()),
            },
            "job": {
                "namespace": self.namespace,
                "name": f"{self.job_prefix}.{table_name}",
                "facets": {
                    "sql": {
                        "_producer": "https://github.com/Seungyeup/RETrend",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SQLJobFacet",
                        "query": sql,
                    }
                },
            },
            "inputs": [{"namespace": ns, "name": name} for ns, name in normalized_inputs],
            "outputs": [
                {
                    "namespace": self.output_namespace,
                    "name": f"{self.output_name_prefix}.{table_name}",
                }
            ],
            "producer": "https://github.com/Seungyeup/RETrend",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        }

        endpoint = build_openlineage_endpoint(self.base_url, self.endpoint)
        resp = self.session.post(endpoint, json=event, timeout=30)

        if 200 <= resp.status_code < 300:
            print(
                "[lineage] emitted "
                f"job={self.job_prefix}.{table_name} inputs={len(normalized_inputs)} "
                f"output={self.output_name_prefix}.{table_name}"
            )
            return

        message = (
            "Failed to emit OpenLineage event: "
            f"status={resp.status_code} body={resp.text[:500]}"
        )
        if self.strict:
            raise RuntimeError(message)
        print(f"[lineage] WARN {message}")


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()

        login_resp = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        login_resp.raise_for_status()
        self.access_token = login_resp.json()["access_token"]
        self.auth_headers = {"Authorization": f"Bearer {self.access_token}"}

        csrf_resp = self.session.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers=self.auth_headers,
            timeout=30,
        )
        csrf_resp.raise_for_status()
        self.csrf_token = csrf_resp.json()["result"]

        self.mutation_headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token,
            "Referer": self.base_url,
        }

    def get_database_id(self, database_name: str) -> int:
        q = json.dumps(
            {"filters": [{"col": "database_name", "opr": "eq", "value": database_name}]}
        )
        resp = self.session.get(
            f"{self.base_url}/api/v1/database/",
            headers=self.auth_headers,
            params={"q": q, "page_size": 100},
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json().get("result", [])
        if not rows:
            raise RuntimeError(f"Superset database not found: {database_name}")
        return int(rows[0]["id"])

    def find_dataset(self, table_name: str, database_id: int) -> Optional[Dict[str, object]]:
        q = json.dumps(
            {"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]}
        )
        resp = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            headers=self.auth_headers,
            params={"q": q, "page_size": 500},
            timeout=30,
        )
        resp.raise_for_status()
        for row in resp.json().get("result", []):
            db = row.get("database", {})
            if int(db.get("id", -1)) == database_id:
                return row
        return None

    def fetch_table_rows(self, dataset_id: int, columns: List[str], row_limit: int = 5000) -> List[Dict[str, object]]:
        query_context = {
            "datasource": {"id": dataset_id, "type": "table"},
            "queries": [
                {
                    "columns": columns,
                    "metrics": [],
                    "filters": [],
                    "is_timeseries": False,
                    "row_limit": row_limit,
                }
            ],
            "result_format": "json",
            "result_type": "full",
        }

        resp = self.session.post(
            f"{self.base_url}/api/v1/chart/data",
            headers=self.mutation_headers,
            json=query_context,
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()["result"][0].get("data", [])

    def upsert_dataset_sql(
        self,
        table_name: str,
        sql: str,
        database_id: int,
        catalog: str = "iceberg",
        schema: str = "default",
    ) -> int:
        existing = self.find_dataset(table_name=table_name, database_id=database_id)
        if existing:
            payload = {
                "table_name": table_name,
                "sql": sql,
                "catalog": catalog,
                "schema": schema,
            }
            resp = self.session.put(
                f"{self.base_url}/api/v1/dataset/{existing['id']}",
                headers=self.mutation_headers,
                json=payload,
                timeout=180,
            )
            resp.raise_for_status()
            existing_id = existing.get("id")
            if existing_id is None:
                raise RuntimeError(f"Existing dataset has no id: {table_name}")
            return int(str(existing_id))

        payload = {
            "database": database_id,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "sql": sql,
        }
        resp = self.session.post(
            f"{self.base_url}/api/v1/dataset/",
            headers=self.mutation_headers,
            json=payload,
            timeout=180,
        )
        resp.raise_for_status()
        body = resp.json()
        dataset_id = body.get("id")
        if dataset_id is None:
            dataset_id = body.get("data", {}).get("id")
        if dataset_id is None:
            raise RuntimeError(f"Dataset create response has no id: {table_name}")
        return int(str(dataset_id))


def build_sgg_map_cte(client: SupersetClient, database_id: int) -> str:
    mapping_dataset_names = [
        "v_retrend_region_last12_ko_fast",
        "v_retrend_region_last12_ko",
    ]
    mapping_rows: List[Dict[str, object]] = []
    for dataset_name in mapping_dataset_names:
        dataset = client.find_dataset(table_name=dataset_name, database_id=database_id)
        if not dataset:
            continue
        dataset_id = dataset.get("id")
        if dataset_id is None:
            continue
        rows = client.fetch_table_rows(int(str(dataset_id)), ["sggcd", "region_ko"], row_limit=5000)
        if rows:
            mapping_rows = rows
            break

    if not mapping_rows:
        return (
            "sgg_map AS (\n"
            "  SELECT sggcd, max_by(estateagentsggnm, deal_month_date) AS region_ko\n"
            "  FROM iceberg.default.apt_trade_features\n"
            "  WHERE estateagentsggnm IS NOT NULL\n"
            "    AND TRIM(estateagentsggnm) <> ''\n"
            "    AND year >= 2024\n"
            "  GROUP BY sggcd\n"
            ")"
        )

    compact: Dict[str, str] = {}
    for row in mapping_rows:
        sggcd = str(row.get("sggcd", "")).strip()
        region_ko = str(row.get("region_ko", "")).strip()
        if not sggcd or not region_ko:
            continue
        if sggcd not in compact:
            compact[sggcd] = region_ko

    values = []
    for sggcd in sorted(compact.keys()):
        region = compact[sggcd].replace("'", "''")
        values.append(f"('{sggcd}', '{region}')")

    values_sql = ",\n    ".join(values)
    return (
        "sgg_map AS (\n"
        "  SELECT * FROM (VALUES\n"
        f"    {values_sql}\n"
        "  ) AS t(sggcd, region_ko)\n"
        ")"
    )


def read_templates(sql_dir: Path) -> Dict[str, str]:
    templates: Dict[str, str] = {}
    for path in sorted(sql_dir.glob("*.sql")):
        table_name = path.stem
        templates[table_name] = path.read_text(encoding="utf-8").strip()
    if not templates:
        raise RuntimeError(f"No SQL files found in {sql_dir}")
    return templates


def placeholder_for_table(table_name: str) -> str:
    return "SQL_" + table_name.upper()


def render_templates(templates: Dict[str, str], sgg_map_cte: str) -> Dict[str, str]:
    rendered: Dict[str, str] = {}
    placeholder_to_table = {placeholder_for_table(t): t for t in templates.keys()}

    def render_one(table_name: str, visiting: Set[str]) -> str:
        if table_name in rendered:
            return rendered[table_name]
        if table_name in visiting:
            raise RuntimeError(f"Circular SQL placeholder dependency detected: {table_name}")
        visiting.add(table_name)

        text = templates[table_name]

        def replace(match: re.Match[str]) -> str:
            token = match.group(1)
            if token == "SGG_MAP_CTE":
                return sgg_map_cte
            ref_table = placeholder_to_table.get(token)
            if not ref_table:
                raise RuntimeError(f"Unknown SQL placeholder: {token} in {table_name}")
            return render_one(ref_table, visiting)

        output = PLACEHOLDER_RE.sub(replace, text).strip()
        visiting.remove(table_name)
        rendered[table_name] = output
        return output

    for table_name in templates.keys():
        render_one(table_name, set())
    return rendered


def main() -> None:
    if not SQL_DIR.exists():
        raise RuntimeError(f"SQL directory does not exist: {SQL_DIR}")

    openlineage_url = os.environ.get("OPENLINEAGE_URL", "").strip()
    openlineage_endpoint = os.environ.get("OPENLINEAGE_ENDPOINT", "/api/v1/lineage")
    openlineage_namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "retrend")
    openlineage_job_prefix = os.environ.get("OPENLINEAGE_JOB_PREFIX", "superset_sql_sync")
    openlineage_output_namespace = os.environ.get(
        "OPENLINEAGE_OUTPUT_NAMESPACE", "superset"
    )
    openlineage_input_namespace_override = os.environ.get(
        "OPENLINEAGE_INPUT_NAMESPACE_OVERRIDE"
    )
    openlineage_output_name_prefix = os.environ.get(
        "OPENLINEAGE_OUTPUT_NAME_PREFIX", "dataset"
    )
    openlineage_skip_no_inputs = env_to_bool(
        os.environ.get("OPENLINEAGE_SKIP_NO_INPUTS"), default=True
    )
    openlineage_enabled = env_to_bool(
        os.environ.get("OPENLINEAGE_EMIT"), default=bool(openlineage_url)
    )
    openlineage_strict = env_to_bool(
        os.environ.get("OPENLINEAGE_STRICT"), default=False
    )

    lineage_emitter = OpenLineageEmitter(
        base_url=openlineage_url,
        endpoint=openlineage_endpoint,
        namespace=openlineage_namespace,
        job_prefix=openlineage_job_prefix,
        output_namespace=openlineage_output_namespace,
        output_name_prefix=openlineage_output_name_prefix,
        input_namespace_override=openlineage_input_namespace_override,
        skip_no_inputs=openlineage_skip_no_inputs,
        enabled=openlineage_enabled,
        strict=openlineage_strict,
    )

    print(
        "[lineage] "
        f"enabled={lineage_emitter.enabled} "
        f"url={build_openlineage_endpoint(openlineage_url, openlineage_endpoint) if openlineage_url else '<not-set>'}"
    )

    client = SupersetClient(base_url=BASE_URL, username=USERNAME, password=PASSWORD)
    database_id = client.get_database_id(DATABASE_NAME)

    templates = read_templates(SQL_DIR)
    sgg_map_cte = build_sgg_map_cte(client=client, database_id=database_id)
    rendered = render_templates(templates=templates, sgg_map_cte=sgg_map_cte)

    print(f"[sync] database_id={database_id}, files={len(rendered)}")
    for table_name, sql in rendered.items():
        dataset_id = client.upsert_dataset_sql(
            table_name=table_name,
            sql=sql,
            database_id=database_id,
            catalog="iceberg",
            schema="default",
        )
        print(f"[sync] {table_name} -> dataset_id={dataset_id}")

        input_datasets = extract_input_datasets(sql)
        lineage_emitter.emit_dataset_lineage(
            table_name=table_name,
            sql=sql,
            input_datasets=input_datasets,
        )


if __name__ == "__main__":
    main()
