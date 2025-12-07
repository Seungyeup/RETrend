# tests/test_client.py
from __future__ import annotations
from kreb_etl.client import fetch_page
from kreb_etl.config import KREBConfig

def make_cfg(**overrides) -> KREBConfig:
    base = {
        "api_bases": ["https://example.com"],
        "api_path": "getAptTrade",
        "service_key": "ABC123",
        "lawd_codes_s3": None,
        "lawd_codes_env": [],
        "shigungu_s3": "s3://dummy/shigungu.csv",
        "start_ym": "202401",
        "end_ym": "202401",
        "sleep_sec": 0.01,
        "retries": 1,
        "backoff": 0.01,
        "limit_codes": 0,
        "limit_months": 0,
        "limit_pages": 0,
        "num_rows": 100,
        "dry_run_sample": False,
        "no_write": True,
        "out_prefix": "s3://dummy/out",
        "requests_headers": {},
    }
    base.update(overrides)
    return KREBConfig(**base)

def test_fetch_page_success(monkeypatch):
    cfg = make_cfg()

    sample_xml = """
    <response>
      <header>
        <resultCode>00</resultCode>
        <resultMsg>NORMAL SERVICE.</resultMsg>
      </header>
      <body>
        <totalCount>2</totalCount>
        <items>
          <item>
            <dealAmount>100,000</dealAmount>
            <buildYear>2000</buildYear>
          </item>
          <item>
            <dealAmount>200,000</dealAmount>
            <buildYear>2010</buildYear>
          </item>
        </items>
      </body>
    </response>
    """

    class FakeResp:
        def __init__(self, text: str, status_code: int = 200):
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP {self.status_code}")

    def fake_get(url, params=None, headers=None, timeout=None):
        # URL/params 체크도 여기서 가능
        assert "https://example.com" in url
        assert params["LAWD_CD"] == "11110"
        assert params["DEAL_YMD"] == "202401"
        return FakeResp(sample_xml, 200)

    monkeypatch.setattr("kreb_etl.client.requests.get", fake_get)

    rows, total = fetch_page(cfg, "11110", "202401", 1)
    assert total == 2
    assert len(rows) == 2
    assert rows[0]["dealAmount"] == "100,000"
    assert rows[1]["buildYear"] == "2010"

def test_fetch_page_api_error(monkeypatch):
    cfg = make_cfg()

    error_xml = """
    <response>
      <header>
        <resultCode>99</resultCode>
        <resultMsg>ERROR</resultMsg>
      </header>
      <body>
        <totalCount>0</totalCount>
      </body>
    </response>
    """

    class FakeResp:
        def __init__(self, text: str, status_code: int = 200):
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP {self.status_code}")

    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResp(error_xml, 200)

    monkeypatch.setattr("kreb_etl.client.requests.get", fake_get)

    import pytest
    with pytest.raises(RuntimeError) as exc:
        fetch_page(cfg, "11110", "202401", 1)

    assert "API error" in str(exc.value)
