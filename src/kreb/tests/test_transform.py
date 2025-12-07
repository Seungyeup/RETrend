# tests/test_transform.py
from __future__ import annotations
from kreb_etl.transform import normalize_rows

def test_normalize_rows_basic():
    rows = [
        {
            "거래금액": "  120,000 ",
            "건축년도": "2005",
            "년": "2024",
            "월": " 5 ",
            "일": " 3 ",
            "전용면적": " 84.97 ",
            "층": " 10 ",
            "other": "  text  ",
        }
    ]
    out = normalize_rows(rows)
    assert len(out) == 1
    r = out[0]

    assert r["거래금액(만원)"] == 120000
    assert r["건축년도"] == 2005
    assert r["년"] == 2024
    assert r["월"] == 5
    assert r["일"] == 3
    assert r["전용면적"] == 84.97
    assert r["층"] == 10
    # 공백 제거 확인
    assert r["other"] == "text"
