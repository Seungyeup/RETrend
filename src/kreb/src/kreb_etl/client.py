from __future__ import annotations
import time
import requests
import xmltodict
from .config import KREBConfig


def fetch_page(cfg: KREBConfig, lawd_cd: str, deal_ym: str, page_no: int):

    if not cfg.service_key:
        raise RuntimeError("KREB_SERVICE_KEY (or SERVICE_KEY) is required")

    key_variants = [cfg.service_key]
    last_exc: Exception | None = None
    url = f"{cfg.api_bases[0]}/{cfg.api_path}"

    for key_idx, key_val in enumerate(key_variants, 1):
        params = {
            "serviceKey": key_val,  # 인증키
            "pageNo": page_no,  # 페이지 번호
            "numOfRows": cfg.num_rows, # 한 페이지 결과 수
            "LAWD_CD": lawd_cd, # 지역코드
            "DEAL_YMD": deal_ym # 계약월
        }

        masked = key_val[:6] + "..." if key_val else ""
        for attempt in range(1, cfg.retries + 1):
            try:
                resp = requests.get(
                    url=url,
                    params=params,
                    headers=cfg.requests_headers,
                    timeout=30,
                )
                resp.raise_for_status()
                data = xmltodict.parse(resp.text)
                header = (((data or {}).get("response", {}) or {}).get("header", {}) or {})
                result_code = str(header.get("resultCode") or "").strip()
                result_msg = str(header.get("resultMsg") or "").strip()

                # TODO: 코드/메세지 config.py 로 분리
                success_codes = {"00", "000"}
                success_msgs = {"OK", "NORMAL SERVICE.", "NORMAL_SERVICE"}

                if result_code and result_code not in success_codes and result_msg not in success_msgs:
                    raise RuntimeError(f"API error {result_code}: {result_msg}")

                body = (((data or {}).get("response", {}) or {}).get("body", {}) or {})
                total_count = (
                    int((body.get("totalCount") or 0))
                    if body.get("totalCount") is not None
                    else None
                )
                items = body.get("items", {})
                item = items.get("item") if isinstance(items, dict) else items
                
                if item is None:
                    return [], total_count
                rows = item if isinstance(item, list) else [item]
                return rows, total_count

            except Exception as e:
                last_exc = e
                if attempt < cfg.retries:
                    time.sleep(cfg.backoff * attempt)
                    print(f"      -> retry {attempt}/{cfg.retries} (key-variant {key_idx}/{len(key_variants)} {masked}) after {cfg.backoff * attempt:.1f}s: {type(e).__name__}: {e}")
                else:
                    break

    raise last_exc if last_exc else RuntimeError("Unknown request error")