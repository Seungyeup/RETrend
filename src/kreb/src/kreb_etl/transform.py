from __future__ import annotations


def normalize_rows(rows: list[dict]) -> list[dict]:
    """
    Normalize rows from KREB API response
    숫자/float 필드 정규화, 공백제거
    """
    
    def to_int(v):
        try:
            return int(str(v).strip().replace(",", ""))
        except Exception:
            return None
        
    def to_float(v):
        try:
            return float(str(v).strip().replace(",", ""))
        except Exception:
            return None

    out: list[dict] = []
    for r in rows:
        rr = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        rr["거래금액(만원)"] = to_int(rr.get("거래금액") or rr.get("dealAmount"))
        rr["건축년도"] = to_int(rr.get("건축년도") or rr.get("buildYear"))
        rr["년"] = to_int(rr.get("년") or rr.get("dealYear"))
        rr["월"] = to_int(rr.get("월") or rr.get("dealMonth"))
        rr["일"] = to_int(rr.get("일") or rr.get("dealDay"))
        rr["전용면적"] = to_float(rr.get("전용면적") or rr.get("exclusiveArea"))
        rr["층"] = to_int(rr.get("층") or rr.get("floor"))
        out.append(rr)
        
    return out