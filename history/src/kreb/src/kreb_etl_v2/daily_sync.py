from __future__ import annotations

"""kreb_etl_v2.daily_sync

Daily quota-limited sync job for KREB apartment trade API.

Goals:
- Run every day with a hard request quota (ex: 10k/day).
- Continuously walk the full keyspace (LAWD_CD x DEAL_YM x page) in a fixed order.
- If API responses change over time, update the already-written page CSVs.
- Persist progress frequently so the next run resumes from the last cursor.
- Print an end-of-run summary including "how many full cycles" and "rough percent position".

Important:
- This module is intentionally standalone: it does NOT import from backfill.
- Output layout is compatible with the existing bronze partition scheme:
    <base>/LAWD_CD=XXXXX/DEAL_YM=YYYYMM/page=N.csv
    <base>/LAWD_CD=XXXXX/DEAL_YM=YYYYMM/_SUCCESS.json

Env vars (same names as backfill where possible):
- KREB_SERVICE_KEY (required)
- KREB_BASE_URL (optional)
- KREB_DAILY_LIMIT (optional; default 10000)
- KREB_LAWD_CSV (required)
- KREB_OUTPUT_URI (required)

State:
- KREB_DAILY_SYNC_STATE_URI (optional; recommended)
  - default: s3://retrend-raw-data/kreb_state_daily_sync.json
- KREB_STATE_URI (fallback only; avoid sharing with backfill)

HTTP tuning:
- KREB_HTTP_RETRIES (default 3)
- KREB_HTTP_BACKOFF_BASE (default 1.0)
- KREB_HTTP_BACKOFF_CAP (default 30.0)
- KREB_HTTP_TIMEOUT_SEC (default 10.0)

MinIO/S3 (when using s3:// URIs):
- MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

import hashlib
import io
import json
import logging
import math
import os
import random
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import fsspec
import pandas as pd
import requests
import s3fs
import xmltodict


# =============================
#  Quota
# =============================


class QuotaExceeded(Exception):
    """Raised when the daily request quota is exhausted."""


class QuotaManager:
    """Simple in-memory quota counter.

    Notes:
    - This is per-process only.
    - For a hard daily quota, we rely on Airflow scheduling (once/day) and
      the state cursor to continue the next day.
    """

    def __init__(self, limit: int):
        self.limit = int(limit)
        self.used = 0

    @property
    def remaining(self) -> int:
        return max(self.limit - self.used, 0)

    def consume(self, n: int = 1) -> None:
        if self.used + n > self.limit:
            raise QuotaExceeded(
                f"quota exceeded: used={self.used}, limit={self.limit}, want={n}"
            )
        self.used += n


# =============================
#  Time/util
# =============================


class RetryableFetchError(RuntimeError):
    """Retryable network/API fetch failure."""


def _now_iso() -> str:
    # UTC timestamp for stable comparisons across environments.
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _md5_hex(data: bytes) -> str:
    h = hashlib.md5()
    h.update(data)
    return h.hexdigest()


def _s3_etag_hex(etag: Optional[str]) -> Optional[str]:
    """Extract plain MD5 from ETag if it looks like a non-multipart MD5."""
    if not etag:
        return None
    s = etag.strip().strip('"')
    # multipart etag looks like: <md5>-<parts>
    if "-" in s:
        return None
    if len(s) != 32:
        return None
    return s


def _is_s3_like_fs(fs: object) -> bool:
    """Best-effort check for S3-like fsspec filesystem."""
    proto = getattr(fs, "protocol", None)
    if proto is None:
        return False
    if isinstance(proto, str):
        return proto in ("s3", "s3a")
    try:
        return "s3" in proto or "s3a" in proto
    except TypeError:
        return False


# =============================
#  Storage helpers (S3/MinIO + local)
# =============================


def build_s3_storage_options() -> Dict[str, Any]:
    """fsspec storage_options for MinIO/S3.

    Uses:
    - MINIO_ENDPOINT
    - MINIO_ACCESS_KEY
    - MINIO_SECRET_KEY
    """

    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not (endpoint and access_key and secret_key):
        return {}

    use_ssl = endpoint.startswith("https://")
    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
        "use_ssl": use_ssl,
    }


def build_s3_fs():
    """Build s3fs.S3FileSystem for MinIO/S3.

    We keep a dedicated s3fs instance because:
    - backfill/daily-sync frequently call fs.exists/info/open
    - s3fs has common semantics for s3:// paths
    """

    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not (endpoint and access_key and secret_key):
        return None

    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
    )


def s3_uri_to_path(uri: str) -> str:
    """Convert s3://bucket/key URI into s3fs path bucket/key."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("s3", "s3a"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return f"{bucket}/{key}" if key else bucket


def load_state(fs, path: str) -> Dict[str, Any]:
    """Load JSON state dict from fs.

    We keep a small stable envelope so the state file is self-descriptive.
    Daily sync stores its fields under state["daily_sync"].
    """

    base: Dict[str, Any] = {
        "version": 2,
        "done": False,
        "done_at": None,
        "cursor": None,
        "last_success": None,
        "last_completed": None,
        "last_error": None,
        "daily_sync": {},
    }

    if not fs.exists(path):
        return dict(base)

    with fs.open(path, "rt", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        return dict(base)

    st = dict(base)
    st.update(raw)
    if "version" not in raw:
        st["version"] = 1
    return st


def save_state(fs, path: str, state: Dict[str, Any]) -> None:
    """Persist state JSON.

    For S3-like FS, a single PUT overwrite is the simplest safe operation.
    For local FS, we use tmp+move to reduce partial-write risks.
    """

    payload = json.dumps(state, ensure_ascii=False)
    if _is_s3_like_fs(fs):
        with fs.open(path, "wt", encoding="utf-8") as f:
            f.write(payload)
        return

    tmp = path + ".tmp"
    with fs.open(tmp, "wt", encoding="utf-8") as f:
        f.write(payload)
    fs.move(tmp, path)


def write_partition_success(
    fs,
    base_path: str,
    lawd_cd: str,
    deal_ym: str,
    manifest: Dict[str, Any],
) -> None:
    """Write per-partition completion marker.

    This is used as a cheap completeness signal without scanning every page file.
    """

    dir_path = f"{base_path}/LAWD_CD={lawd_cd}/DEAL_YM={deal_ym}"
    marker_path = f"{dir_path}/_SUCCESS.json"

    if fs.exists(marker_path):
        try:
            info = fs.info(marker_path)
            if int(info.get("size") or 0) > 0:
                return
        except Exception:
            return

    fs.mkdirs(dir_path, exist_ok=True)
    with fs.open(marker_path, "wt", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False)


# =============================
#  Domain helpers: months & LAWD list
# =============================


def generate_last_10y_months(today: Optional[dt.date] = None) -> List[str]:
    """Return YYYYMM list for the last 10 years (oldest -> newest)."""
    today = today or dt.date.today()
    year, month = today.year, today.month
    arr: List[str] = []
    for _ in range(12 * 10):
        arr.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return sorted(arr)


def load_lawd_codes_from_csv(path: str, storage_options: Dict[str, Any]) -> List[str]:
    """Load LAWD_CD list from CSV (local or s3://).

    The CSV is expected to contain one of these columns:
    - LAWD_CD
    - lawd_cd
    - cortarNo
    """

    read_kwargs: Dict[str, Any] = {"dtype": str}
    if path.startswith(("s3://", "s3a://")) and storage_options:
        read_kwargs["storage_options"] = storage_options

    df = pd.read_csv(path, **read_kwargs)
    for col in ("LAWD_CD", "lawd_cd", "cortarNo"):
        if col in df.columns:
            return sorted(
                df[col]
                .astype(str)
                .str.strip()
                .str.slice(0, 5)
                .dropna()
                .unique()
                .tolist()
            )
    raise ValueError(f"{path} 에서 LAWD_CD 컬럼을 찾을 수 없습니다.")


# =============================
#  KREB API client + pagination
# =============================


class KrebClient:
    """KREB apartment trade API client.

    Design choices:
    - Every HTTP request consumes quota.
    - 429 is treated as quota exhaustion.
    - timeouts/connection errors/5xx/empty-middle-page are retryable.
    """

    def __init__(
        self,
        service_key: str,
        base_url: str,
        num_of_rows: int,
        quota: QuotaManager,
        http_retries: int = 3,
        http_backoff_base: float = 1.0,
        http_backoff_cap: float = 30.0,
        http_timeout_sec: float = 10.0,
    ) -> None:
        self.service_key = service_key
        self.base_url = base_url
        self.num_of_rows = num_of_rows
        self.quota = quota

        self.http_retries = max(int(http_retries), 0)
        self.http_backoff_base = float(http_backoff_base)
        self.http_backoff_cap = float(http_backoff_cap)
        self.http_timeout_sec = float(http_timeout_sec)

    def fetch_page(
        self, lawd_cd: str, deal_ym: str, page_no: int
    ) -> Tuple[int, int, int, int, List[Dict[str, Any]]]:
        """Fetch a single page.

        Returns:
          total_count, num_of_rows, total_pages, page_no_resp, items
        """

        params = {
            "serviceKey": self.service_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ym,
            "pageNo": page_no,
            "numOfRows": self.num_of_rows,
        }

        last_exc: Optional[BaseException] = None
        for attempt in range(self.http_retries + 1):
            try:
                self.quota.consume(1)
                resp = requests.get(
                    self.base_url, params=params, timeout=self.http_timeout_sec
                )

                if resp.status_code == 429:
                    raise QuotaExceeded(
                        f"HTTP 429 Too Many Requests: lawd_cd={lawd_cd}, deal_ym={deal_ym}, page_no={page_no}"
                    )

                if 500 <= resp.status_code < 600:
                    raise RetryableFetchError(
                        f"HTTP {resp.status_code}: lawd_cd={lawd_cd}, deal_ym={deal_ym}, page_no={page_no}"
                    )

                try:
                    resp.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    # Avoid logging full URL (serviceKey may appear in querystring).
                    raise requests.exceptions.HTTPError(
                        f"HTTP {resp.status_code} error: lawd_cd={lawd_cd}, deal_ym={deal_ym}, page_no={page_no}"
                    ) from e

                doc = xmltodict.parse(resp.text)
                body = doc.get("response", {}).get("body", {}) or {}

                total = int(body.get("totalCount", 0) or 0)
                rows = int(body.get("numOfRows", self.num_of_rows) or self.num_of_rows)
                page = int(body.get("pageNo", page_no) or page_no)

                if total <= 0:
                    total_pages = 1
                else:
                    total_pages = max(math.ceil(total / max(rows, 1)), 1)

                items = body.get("items", {})
                raw = items.get("item") if isinstance(items, dict) else None

                if raw is None:
                    parsed: List[Dict[str, Any]] = []
                elif isinstance(raw, list):
                    parsed = [dict(x) for x in raw]
                else:
                    parsed = [dict(raw)]

                # Defensive: if total>0 and we are not at the last page yet, an empty page
                # is more likely a transient/parse issue than "real empty".
                if total > 0 and page < total_pages and not parsed:
                    raise RetryableFetchError(
                        f"empty items before last page: lawd_cd={lawd_cd}, deal_ym={deal_ym}, page_no={page}, total_pages={total_pages}"
                    )

                return total, rows, total_pages, page, parsed

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                RetryableFetchError,
            ) as e:
                last_exc = e
                if attempt >= self.http_retries:
                    break
                backoff = min(
                    self.http_backoff_base * (2**attempt), self.http_backoff_cap
                )
                jitter = backoff * (0.1 * random.random())
                time.sleep(backoff + jitter)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("unknown fetch error")


def iter_month_pages(
    client: KrebClient, lawd_cd: str, deal_ym: str, start_page: int = 1
):
    """Iterate pages for a single (LAWD_CD, DEAL_YM) pair.

    Yields:
      (page_no, total_pages, items)
    """

    page = start_page
    while True:
        total, rows, total_pages, page_no, items = client.fetch_page(
            lawd_cd, deal_ym, page
        )
        yield page_no, total_pages, items
        if page_no >= total_pages:
            break
        page += 1


# =============================
#  Output writer: update-on-change page CSV
# =============================


def write_page_csv_update(
    fs,
    base_path: str,
    lawd_cd: str,
    deal_ym: str,
    page_no: int,
    items: List[Dict[str, Any]],
) -> Tuple[str, Optional[str]]:
    """Write page CSV and update if content changed.

    Return:
      (action, md5)

    action:
      - created: file did not exist
      - updated: existed but content changed
      - unchanged: existed and content identical
      - skipped_empty: items was empty
    """

    if not items:
        return "skipped_empty", None

    df = pd.DataFrame(items)
    dir_path = f"{base_path}/LAWD_CD={lawd_cd}/DEAL_YM={deal_ym}"
    file_path = f"{dir_path}/page={page_no}.csv"
    fs.mkdirs(dir_path, exist_ok=True)

    # We compute MD5 over the actual bytes we will write.
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8-sig")
    new_md5 = _md5_hex(data)

    if fs.exists(file_path):
        # Try cheap compare first (S3 ETag); otherwise fall back to reading bytes.
        try:
            info = fs.info(file_path)
            etag = info.get("ETag") if isinstance(info, dict) else None
            existing_etag = _s3_etag_hex(etag)
            if existing_etag and existing_etag == new_md5:
                return "unchanged", new_md5

            if not existing_etag:
                with fs.open(file_path, "rb") as f:
                    old = f.read()
                if _md5_hex(old) == new_md5:
                    return "unchanged", new_md5
        except Exception:
            # If comparison fails, we still overwrite to keep the job progressing.
            pass

        with fs.open(file_path, "wb") as f:
            f.write(data)
        return "updated", new_md5

    with fs.open(file_path, "wb") as f:
        f.write(data)
    return "created", new_md5


# =============================
#  Progress math
# =============================


def _compute_slot_index(
    lawd_list: List[str],
    ym_list: List[str],
    cursor: Optional[Dict[str, Any]],
) -> int:
    """Convert cursor (lawd_cd, deal_ym) to linear slot index."""
    if not cursor:
        return 0
    lawd_cd = cursor.get("lawd_cd")
    deal_ym = cursor.get("deal_ym")
    if not (lawd_cd and deal_ym):
        return 0
    try:
        lawd_idx = lawd_list.index(str(lawd_cd))
        ym_idx = ym_list.index(str(deal_ym))
        return ym_idx * len(lawd_list) + lawd_idx
    except ValueError:
        return 0


def _compute_percent_position(
    total_slots: int,
    slot_idx: int,
    page_no: Optional[int],
    total_pages: Optional[int],
) -> float:
    """Compute rough percent position within the full range.

    This is not exact progress-by-record; it's a stable, cheap approximation.
    """
    if total_slots <= 0:
        return 0.0
    slot_idx = max(min(slot_idx, total_slots), 0)
    base = float(slot_idx) / float(total_slots)

    if not page_no or not total_pages or total_pages <= 0:
        return base * 100.0

    frac = (max(page_no, 1) - 1) / float(max(total_pages, 1))
    fine = frac / float(total_slots)
    return (base + fine) * 100.0


def _default_daily_sync_state_uri() -> str:
    return "s3://retrend-raw-data/kreb_state_daily_sync.json"


# =============================
#  Main entry
# =============================


def run_daily_sync_once() -> None:
    """Run one quota-bounded daily sync.

    Behavior:
    - Resume from state.daily_sync.cursor.
    - Walk newest months first.
    - For each page, write/overwrite page CSV if content changed.
    - After finishing a partition, write _SUCCESS.json.
    - If it reaches the end, increment cycles_completed and wrap to the beginning.
    - On exit, always write a summary into state.daily_sync.last_run and log it.
    """

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
    )
    log = logging.getLogger("kreb_etl_v2.daily_sync")

    service_key = os.environ.get("KREB_SERVICE_KEY")
    if not service_key:
        raise ValueError("환경변수 KREB_SERVICE_KEY 가 필요합니다.")

    base_url = os.environ.get(
        "KREB_BASE_URL",
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    )
    daily_limit = int(os.environ.get("KREB_DAILY_LIMIT", "10000"))

    lawd_csv = os.environ.get("KREB_LAWD_CSV")
    if not lawd_csv:
        raise ValueError("환경변수 KREB_LAWD_CSV 가 필요합니다. (LAWD 코드 CSV 경로)")

    output_uri = os.environ.get("KREB_OUTPUT_URI")
    if not output_uri:
        raise ValueError("환경변수 KREB_OUTPUT_URI 가 필요합니다. (CSV 저장 base 경로)")

    # Prefer a dedicated state file for daily sync.
    state_uri = os.environ.get("KREB_DAILY_SYNC_STATE_URI") or os.environ.get(
        "KREB_STATE_URI", _default_daily_sync_state_uri()
    )

    quota = QuotaManager(daily_limit)

    http_retries = int(os.environ.get("KREB_HTTP_RETRIES", "3"))
    http_backoff_base = float(os.environ.get("KREB_HTTP_BACKOFF_BASE", "1.0"))
    http_backoff_cap = float(os.environ.get("KREB_HTTP_BACKOFF_CAP", "30.0"))
    http_timeout_sec = float(os.environ.get("KREB_HTTP_TIMEOUT_SEC", "10.0"))

    client = KrebClient(
        service_key,
        base_url,
        num_of_rows=1000,
        quota=quota,
        http_retries=http_retries,
        http_backoff_base=http_backoff_base,
        http_backoff_cap=http_backoff_cap,
        http_timeout_sec=http_timeout_sec,
    )

    storage_options = build_s3_storage_options()
    log.info("storage_options=%s", storage_options)

    s3_fs = build_s3_fs()

    # Resolve state FS
    if state_uri.startswith(("s3://", "s3a://")):
        if s3_fs is None:
            raise RuntimeError("S3 state_uri 를 쓰려면 MINIO_* 환경변수가 필요합니다.")
        fs_state = s3_fs
        state_path = s3_uri_to_path(state_uri)
    else:
        fs_state, state_path = fsspec.core.url_to_fs(state_uri)

    # Resolve output FS
    if output_uri.startswith(("s3://", "s3a://")):
        if s3_fs is None:
            raise RuntimeError("S3 output_uri 를 쓰려면 MINIO_* 환경변수가 필요합니다.")
        fs_out = s3_fs
        out_base_path = s3_uri_to_path(output_uri)
    else:
        fs_out, out_base_path = fsspec.core.url_to_fs(output_uri)

    # Load/initialize state
    state = load_state(fs_state, state_path)
    daily_sync = state.get("daily_sync")
    if not isinstance(daily_sync, dict):
        daily_sync = {}

    cycles_completed = int(daily_sync.get("cycles_completed") or 0)
    cursor = daily_sync.get("cursor")
    if cursor is not None and not isinstance(cursor, dict):
        cursor = None

    # Newest months first: daily sync wants to refresh recent data more often.
    ym_list = list(reversed(generate_last_10y_months()))
    lawd_list = load_lawd_codes_from_csv(lawd_csv, storage_options)
    total_slots = len(lawd_list) * len(ym_list)
    start_idx = _compute_slot_index(lawd_list, ym_list, cursor)

    log.info(
        "Start daily sync: cycles_completed=%s, total_slots=%s, start_idx=%s, daily_limit=%s",
        cycles_completed,
        total_slots,
        start_idx,
        daily_limit,
    )

    run_started_at = _now_iso()
    pages_fetched = 0
    partitions_completed = 0
    created = 0
    updated = 0
    unchanged = 0
    skipped_empty = 0

    # Partitions fully completed in this run.
    # This list is used by the downstream Spark job (incremental Iceberg reflect).
    run_completed_partitions: List[Dict[str, str]] = []

    for idx in range(start_idx, total_slots):
        if quota.remaining <= 0:
            break

        ym_idx, lawd_idx = divmod(idx, len(lawd_list))
        lawd_cd = lawd_list[lawd_idx]
        deal_ym = ym_list[ym_idx]

        start_page = 1
        if (
            cursor
            and cursor.get("lawd_cd") == lawd_cd
            and cursor.get("deal_ym") == deal_ym
        ):
            start_page = int(cursor.get("page_no") or 1)

        log.info(
            "Slot start: cycle=%s, lawd_cd=%s, deal_ym=%s, start_page=%s, remaining=%s",
            cycles_completed,
            lawd_cd,
            deal_ym,
            start_page,
            quota.remaining,
        )

        month_total_pages: Optional[int] = None
        last_month_page: Optional[int] = None
        last_page_md5: Optional[str] = None

        try:
            for page_no, total_pages, items in iter_month_pages(
                client, lawd_cd, deal_ym, start_page
            ):
                action, md5hex = write_page_csv_update(
                    fs_out, out_base_path, lawd_cd, deal_ym, page_no, items
                )
                if action == "created":
                    created += 1
                elif action == "updated":
                    updated += 1
                elif action == "unchanged":
                    unchanged += 1
                elif action == "skipped_empty":
                    skipped_empty += 1

                pages_fetched += 1
                last_page_md5 = md5hex

                # Cursor means "next page to fetch".
                cursor = {
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "page_no": page_no + 1,
                }
                daily_sync["cursor"] = cursor

                percent = _compute_percent_position(
                    total_slots, idx, page_no, total_pages
                )
                daily_sync["last_success"] = {
                    "ts": _now_iso(),
                    "cycle": cycles_completed,
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "page_no": page_no,
                    "total_pages": total_pages,
                    "items_count": len(items),
                    "page_md5": last_page_md5,
                    "percent": percent,
                    "quota_used": quota.used,
                    "quota_remaining": quota.remaining,
                }
                daily_sync["last_error"] = None

                state["daily_sync"] = daily_sync
                save_state(fs_state, state_path, state)

                month_total_pages = total_pages
                last_month_page = page_no

        except QuotaExceeded as e:
            # Quota stop is a "normal" end condition for daily runs.
            daily_sync["last_error"] = {
                "ts": _now_iso(),
                "type": type(e).__name__,
                "message": str(e),
                "cycle": cycles_completed,
                "lawd_cd": lawd_cd,
                "deal_ym": deal_ym,
                "page_no": start_page,
            }
            state["daily_sync"] = daily_sync
            save_state(fs_state, state_path, state)
            break
        except Exception as e:
            # Any non-quota error should be visible to operators.
            daily_sync["last_error"] = {
                "ts": _now_iso(),
                "type": type(e).__name__,
                "message": str(e),
                "cycle": cycles_completed,
                "lawd_cd": lawd_cd,
                "deal_ym": deal_ym,
                "page_no": start_page,
            }
            state["daily_sync"] = daily_sync
            save_state(fs_state, state_path, state)
            raise

        # Partition completion: reached the last page.
        if (
            month_total_pages is not None
            and last_month_page is not None
            and last_month_page >= month_total_pages
        ):
            partitions_completed += 1

            run_completed_partitions.append({"lawd_cd": lawd_cd, "deal_ym": deal_ym})

            write_partition_success(
                fs_out,
                out_base_path,
                lawd_cd,
                deal_ym,
                {
                    "ts": _now_iso(),
                    "cycle": cycles_completed,
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "total_pages": month_total_pages,
                    "quota_used": quota.used,
                },
            )

            # Advance cursor to the next slot.
            if idx >= total_slots - 1:
                # Completed one full sweep.
                cycles_completed += 1
                daily_sync["cycles_completed"] = cycles_completed
                if total_slots > 0:
                    cursor = {
                        "lawd_cd": lawd_list[0],
                        "deal_ym": ym_list[0],
                        "page_no": 1,
                    }
                    daily_sync["cursor"] = cursor
            else:
                next_ym_idx, next_lawd_idx = divmod(idx + 1, len(lawd_list))
                cursor = {
                    "lawd_cd": lawd_list[next_lawd_idx],
                    "deal_ym": ym_list[next_ym_idx],
                    "page_no": 1,
                }
                daily_sync["cursor"] = cursor

            daily_sync["last_completed"] = {
                "ts": _now_iso(),
                "cycle": cycles_completed,
                "lawd_cd": lawd_cd,
                "deal_ym": deal_ym,
                "total_pages": month_total_pages,
            }
            state["daily_sync"] = daily_sync
            save_state(fs_state, state_path, state)

    # End-of-run summary (operator-facing)
    end_slot_idx = _compute_slot_index(lawd_list, ym_list, cursor)
    percent_end = _compute_percent_position(total_slots, end_slot_idx, None, None)

    # Write a manifest into bronze so Spark can consume it without accessing the state store.
    # Spark reads from S3A (s3a://...), but the path layout is identical.
    manifest_dir = f"{out_base_path}/_manifests/daily_sync"
    manifest_path = f"{manifest_dir}/latest.json"
    try:
        fs_out.mkdirs(manifest_dir, exist_ok=True)
        with fs_out.open(manifest_path, "wt", encoding="utf-8") as f:
            json.dump(
                {
                    "ts": _now_iso(),
                    "started_at": run_started_at,
                    "cycles_completed": cycles_completed,
                    "cursor": cursor,
                    "percent": percent_end,
                    "quota_used": quota.used,
                    "quota_limit": quota.limit,
                    "completed_partitions": run_completed_partitions,
                },
                f,
                ensure_ascii=False,
            )
    except Exception:
        # Manifest is an optimization. State still contains the run summary.
        manifest_path = None
    daily_sync["last_run"] = {
        "ts": _now_iso(),
        "started_at": run_started_at,
        "cycles_completed": cycles_completed,
        "cursor": cursor,
        "percent": percent_end,
        "quota_used": quota.used,
        "quota_limit": quota.limit,
        "pages_fetched": pages_fetched,
        "partitions_completed": partitions_completed,
        "completed_partitions": run_completed_partitions,
        "manifest_path": manifest_path,
        "writes": {
            "created": created,
            "updated": updated,
            "unchanged": unchanged,
            "skipped_empty": skipped_empty,
        },
    }
    daily_sync["cycles_completed"] = cycles_completed
    state["daily_sync"] = daily_sync
    save_state(fs_state, state_path, state)

    log.info(
        "Daily sync summary: cycles_completed=%s, cursor=%s, percent=%.2f, quota_used=%s/%s, pages=%s, partitions=%s, writes(created=%s updated=%s unchanged=%s empty=%s)",
        cycles_completed,
        cursor,
        percent_end,
        quota.used,
        quota.limit,
        pages_fetched,
        partitions_completed,
        created,
        updated,
        unchanged,
        skipped_empty,
    )


if __name__ == "__main__":
    run_daily_sync_once()
