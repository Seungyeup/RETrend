from __future__ import annotations

import fsspec


def test_compute_slot_index_and_percent():
    from kreb_etl_v2.daily_sync import _compute_percent_position, _compute_slot_index

    lawd = ["11110", "22220"]
    ym = ["202402", "202401"]

    assert _compute_slot_index(lawd, ym, None) == 0
    assert _compute_slot_index(lawd, ym, {"lawd_cd": "11110", "deal_ym": "202402"}) == 0
    assert _compute_slot_index(lawd, ym, {"lawd_cd": "22220", "deal_ym": "202402"}) == 1
    assert _compute_slot_index(lawd, ym, {"lawd_cd": "11110", "deal_ym": "202401"}) == 2

    pct0 = _compute_percent_position(4, 0, 1, 2)
    pct1 = _compute_percent_position(4, 0, 2, 2)
    assert pct0 < pct1


def test_write_page_csv_update_actions():
    from kreb_etl_v2.daily_sync import write_page_csv_update

    fs = fsspec.filesystem("memory")
    base = "/out"

    action1, md1 = write_page_csv_update(
        fs, base, "11110", "202401", 1, [{"a": "1", "b": "2"}]
    )
    assert action1 == "created"
    assert md1

    action2, md2 = write_page_csv_update(
        fs, base, "11110", "202401", 1, [{"a": "1", "b": "2"}]
    )
    assert action2 in ("unchanged", "updated")
    assert md2 == md1

    action3, md3 = write_page_csv_update(
        fs, base, "11110", "202401", 1, [{"a": "9", "b": "2"}]
    )
    assert action3 == "updated"
    assert md3
    assert md3 != md1
