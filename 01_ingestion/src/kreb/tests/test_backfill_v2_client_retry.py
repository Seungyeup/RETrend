from __future__ import annotations


def test_kreb_client_retries_on_timeout(monkeypatch):
    from kreb_etl_v2.backfill import KrebClient, QuotaManager

    calls = {"n": 0}

    sample_xml = """
    <response>
      <body>
        <totalCount>1</totalCount>
        <numOfRows>1</numOfRows>
        <pageNo>1</pageNo>
        <items>
          <item>
            <dealAmount>100,000</dealAmount>
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
                raise RuntimeError(f"HTTP {self.status_code}")

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        if calls["n"] <= 2:
            import requests

            raise requests.exceptions.Timeout("timeout")
        return FakeResp(sample_xml, 200)

    monkeypatch.setattr("kreb_etl_v2.backfill.requests.get", fake_get)
    monkeypatch.setattr("kreb_etl_v2.backfill.time.sleep", lambda _: None)

    quota = QuotaManager(limit=100)
    client = KrebClient(
        service_key="X",
        base_url="https://example.com",
        num_of_rows=1,
        quota=quota,
        http_retries=3,
        http_backoff_base=0.0,
        http_backoff_cap=0.0,
        http_timeout_sec=0.01,
    )

    total, rows, total_pages, page, items = client.fetch_page("11110", "202401", 1)
    assert total == 1
    assert rows == 1
    assert total_pages == 1
    assert page == 1
    assert items[0]["dealAmount"] == "100,000"
    assert calls["n"] == 3


def test_kreb_client_retries_on_empty_items_before_last_page(monkeypatch):
    from kreb_etl_v2.backfill import KrebClient, QuotaManager

    calls = {"n": 0}

    xml_empty = """
    <response>
      <body>
        <totalCount>2</totalCount>
        <numOfRows>1</numOfRows>
        <pageNo>1</pageNo>
        <items></items>
      </body>
    </response>
    """

    xml_ok = """
    <response>
      <body>
        <totalCount>2</totalCount>
        <numOfRows>1</numOfRows>
        <pageNo>1</pageNo>
        <items>
          <item>
            <dealAmount>100,000</dealAmount>
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
                raise RuntimeError(f"HTTP {self.status_code}")

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        return FakeResp(xml_empty if calls["n"] == 1 else xml_ok, 200)

    monkeypatch.setattr("kreb_etl_v2.backfill.requests.get", fake_get)
    monkeypatch.setattr("kreb_etl_v2.backfill.time.sleep", lambda _: None)

    quota = QuotaManager(limit=100)
    client = KrebClient(
        service_key="X",
        base_url="https://example.com",
        num_of_rows=1,
        quota=quota,
        http_retries=1,
        http_backoff_base=0.0,
        http_backoff_cap=0.0,
        http_timeout_sec=0.01,
    )

    total, rows, total_pages, page, items = client.fetch_page("11110", "202401", 1)
    assert total == 2
    assert total_pages == 2
    assert page == 1
    assert len(items) == 1
    assert calls["n"] == 2
