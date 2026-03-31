from pathlib import Path
from unittest.mock import Mock
import importlib.util


MODULE_PATH = Path(__file__).resolve().parents[1] / "sync_superset_sql.py"
SPEC = importlib.util.spec_from_file_location("sync_superset_sql", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Failed to load sync_superset_sql module for tests")

MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

OpenLineageEmitter = MODULE.OpenLineageEmitter
build_openlineage_endpoint = MODULE.build_openlineage_endpoint
env_to_bool = MODULE.env_to_bool
extract_input_datasets = MODULE.extract_input_datasets


def test_env_to_bool_values():
    assert env_to_bool("true") is True
    assert env_to_bool("1") is True
    assert env_to_bool("false") is False
    assert env_to_bool("0") is False
    assert env_to_bool(None, default=True) is True


def test_extract_input_datasets_from_and_join():
    sql = """
    WITH x AS (
      SELECT *
      FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1
    )
    SELECT *
    FROM iceberg.default.apt_trade_region_median_ppsqm_last12m_v1 r
    JOIN iceberg.default.apt_trade_complex_area_bucket_median_ppsqm_12m_v1 c
      ON r.sggcd = c.sggcd
    """

    datasets = extract_input_datasets(sql)
    assert ("iceberg", "default.apt_trade_region_monthly_ppsqm_stats_15y_v1") in datasets
    assert ("iceberg", "default.apt_trade_region_median_ppsqm_last12m_v1") in datasets
    assert ("iceberg", "default.apt_trade_complex_area_bucket_median_ppsqm_12m_v1") in datasets


def test_build_openlineage_endpoint_normalization():
    assert (
        build_openlineage_endpoint(
            "http://marquez.openlineage.svc.cluster.local:5000", "/api/v1/lineage"
        )
        == "http://marquez.openlineage.svc.cluster.local:5000/api/v1/lineage"
    )
    assert (
        build_openlineage_endpoint(
            "http://marquez.openlineage.svc.cluster.local:5000/api/v1/lineage",
            "/api/v1/lineage",
        )
        == "http://marquez.openlineage.svc.cluster.local:5000/api/v1/lineage"
    )


def test_openlineage_emitter_posts_event():
    emitter = OpenLineageEmitter(
        base_url="http://marquez.openlineage.svc.cluster.local:5000",
        endpoint="/api/v1/lineage",
        namespace="retrend",
        job_prefix="superset_sql_sync",
        output_namespace="superset",
        output_name_prefix="dataset",
        input_namespace_override=None,
        skip_no_inputs=True,
        enabled=True,
        strict=True,
    )

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.text = "ok"

    emitter.session.post = Mock(return_value=mock_resp)

    emitter.emit_dataset_lineage(
        table_name="v_retrend_monthly_trades_fast",
        sql="SELECT * FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1",
        input_datasets=[("iceberg", "default.apt_trade_region_monthly_ppsqm_stats_15y_v1")],
    )

    emitter.session.post.assert_called_once()
    call_kwargs = emitter.session.post.call_args.kwargs
    assert call_kwargs["timeout"] == 30
    assert call_kwargs["json"]["eventType"] == "COMPLETE"
    assert call_kwargs["json"]["job"]["name"] == "superset_sql_sync.v_retrend_monthly_trades_fast"


def test_openlineage_emitter_skips_when_no_inputs():
    emitter = OpenLineageEmitter(
        base_url="http://marquez.openlineage.svc.cluster.local:5000",
        endpoint="/api/v1/lineage",
        namespace="retrend",
        job_prefix="superset_sql_sync",
        output_namespace="superset",
        output_name_prefix="dataset",
        input_namespace_override=None,
        skip_no_inputs=True,
        enabled=True,
        strict=False,
    )

    emitter.session.post = Mock()

    emitter.emit_dataset_lineage(
        table_name="v_retrend_region_decision_latest_fast",
        sql="SELECT 1",
        input_datasets=[],
    )

    emitter.session.post.assert_not_called()
