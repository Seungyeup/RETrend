from __future__ import annotations

import fsspec


def test_state_roundtrip_defaults_and_persistence():
    from kreb_etl_v2.backfill import load_state, save_state

    fs = fsspec.filesystem("memory")
    path = "/kreb_state.json"

    st = load_state(fs, path)
    assert st["version"] == 2
    assert st["done"] is False
    assert st["cursor"] is None

    st["cursor"] = {"lawd_cd": "11110", "deal_ym": "202401", "page_no": 3}
    save_state(fs, path, st)

    st2 = load_state(fs, path)
    assert st2["cursor"]["lawd_cd"] == "11110"
    assert st2["cursor"]["page_no"] == 3


def test_partition_success_marker_written():
    from kreb_etl_v2.backfill import write_partition_success

    fs = fsspec.filesystem("memory")
    base = "/out"
    write_partition_success(
        fs,
        base,
        "11110",
        "202401",
        {"ok": True},
    )
    assert fs.exists("/out/LAWD_CD=11110/DEAL_YM=202401/_SUCCESS.json")
