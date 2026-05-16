from __future__ import annotations


def test_iter_month_pages_uses_total_pages_not_empty_items():
    from kreb_etl_v2.backfill import iter_month_pages

    class StubClient:
        def __init__(self):
            self.calls = []

        def fetch_page(self, lawd_cd: str, deal_ym: str, page_no: int):
            self.calls.append(page_no)
            # total_pages=3, and page 2 intentionally empty
            if page_no == 2:
                return 3, 1, 3, 2, []
            return 3, 1, 3, page_no, [{"x": str(page_no)}]

    c = StubClient()
    out = list(iter_month_pages(c, "11110", "202401", 1))
    assert [p for (p, _tp, _items) in out] == [1, 2, 3]
    assert c.calls == [1, 2, 3]
