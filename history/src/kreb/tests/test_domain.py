# tests/test_domain.py
from __future__ import annotations
import pandas as pd
from kreb_etl.domain import months_between, load_lawd_codes
from kreb_etl.config import KREBConfig, S3Config

class DummyCfg(KREBConfig):
    """테스트용 KREBConfig 상속 or 그냥 dataclass 직접 만들어도 됨."""
    pass

def make_cfg(**overrides) -> KREBConfig:
    base = {
        "api_bases": ["https://example.com"],
        "api_path": "dummy",
        "service_key": "ABC123",
        "lawd_codes_s3": None,
        "lawd_codes_env": [],
        "shigungu_s3": "s3://dummy/shigungu.csv",
        "start_ym": "202401",
        "end_ym": "202403",
        "sleep_sec": 0.1,
        "retries": 1,
        "backoff": 0.1,
        "limit_codes": 0,
        "limit_months": 0,
        "limit_pages": 0,
        "num_rows": 10,
        "dry_run_sample": False,
        "no_write": True,
        "out_prefix": "s3://dummy/out",
        "requests_headers": {},
    }
    base.update(overrides)
    return KREBConfig(**base)

def test_months_between_basic():
    assert months_between("202401", "202403") == ["202401", "202402", "202403"]
    assert months_between("202412", "202501") == ["202412", "202501"]
    assert months_between("", "202401") == []
    assert months_between("202401", "") == []

def test_load_lawd_codes_from_env(monkeypatch):
    cfg = make_cfg(lawd_codes_env=["11110", "22220"])
    s3 = S3Config(endpoint=None, access_key=None, secret_key=None)

    # pd.read_csv 가 호출되면 안 되는지 확인해도 좋고, 그냥 넘어가도 됨.
    codes = load_lawd_codes(cfg, s3)
    assert codes == ["11110", "22220"]

def test_load_lawd_codes_from_lawd_csv(monkeypatch):
    # env 비우고 CSV 사용
    cfg = make_cfg(
        lawd_codes_env=[],
        lawd_codes_s3="s3://dummy/lawd_codes.csv",
    )
    s3 = S3Config(endpoint=None, access_key=None, secret_key=None)

    # read_csv mocking
    def fake_read_csv(path, storage_options=None):
        assert path == "s3://dummy/lawd_codes.csv"
        return pd.DataFrame({"lawd_cd": ["111101234", "222202222"]})

    monkeypatch.setattr("kreb_etl.domain.pd.read_csv", fake_read_csv)

    codes = load_lawd_codes(cfg, s3)
    assert codes == ["11110", "22220"]

def test_load_lawd_codes_from_shigungu(monkeypatch):
    cfg = make_cfg(
        lawd_codes_env=[],
        lawd_codes_s3=None,
        shigungu_s3="s3://dummy/shigungu.csv",
    )
    s3 = S3Config(endpoint=None, access_key=None, secret_key=None)

    def fake_read_csv(path, storage_options=None):
        assert path == "s3://dummy/shigungu.csv"
        return pd.DataFrame({
            "cortarNo": ["333303333", "444404444"],
            "name": ["A동", "B동"],
        })

    monkeypatch.setattr("kreb_etl.domain.pd.read_csv", fake_read_csv)

    codes = load_lawd_codes(cfg, s3)
    assert codes == ["33330", "44440"]
