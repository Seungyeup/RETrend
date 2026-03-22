# src/kreb_etl_v2/backfill.py
import os
import math
import json
import logging
import datetime as dt
import time
import random

from typing import Any, Dict, List, Optional, Tuple

import requests
import xmltodict
import fsspec
import pandas as pd
from urllib.parse import urlparse
import s3fs


# =============================
#  Quota (일일 호출 제한)
# =============================
class QuotaExceeded(Exception):
    """일일 호출 한도를 초과했을 때 발생하는 예외."""

    pass


class RetryableFetchError(RuntimeError):
    """재시도로 복구 가능성이 있는 fetch 실패."""


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _is_s3_like_fs(fs: object) -> bool:
    proto = getattr(fs, "protocol", None)
    if proto is None:
        return False
    if isinstance(proto, str):
        return proto in ("s3", "s3a")
    try:
        return "s3" in proto or "s3a" in proto
    except TypeError:
        return False


class QuotaManager:
    """
    아주 단순한 in-memory 쿼터 매니저.
    - 이 프로세스(컨테이너) 안에서만 유효한 일일 호출 제한 관리.
    """

    def __init__(self, limit: int):
        self.limit = limit
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
#  KREB API Client
# =============================


class KrebClient:
    """
    KREB 아파트 매매 실거래가 API 클라이언트.
    - 모든 HTTP 호출은 QuotaManager.consume() 을 반드시 거친다.
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
        self,
        lawd_cd: str,
        deal_ym: str,
        page_no: int,
    ) -> Tuple[int, int, int, int, List[Dict[str, Any]]]:
        """
        (lawd_cd, deal_ym) 에 대한 특정 pageNo를 호출한다.

        Returns:
            total_count, num_of_rows, total_pages, page_no_resp, items(list[dict])
        """
        params = {
            "serviceKey": self.service_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ym,  # YYYYMM
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

                # Too Many Requests (429) → QuotaExceeded 로 치환
                if resp.status_code == 429:
                    raise QuotaExceeded(
                        f"HTTP 429 Too Many Requests: lawd_cd={lawd_cd}, "
                        f"deal_ym={deal_ym}, page_no={page_no}"
                    )

                # 5xx는 재시도 대상
                if 500 <= resp.status_code < 600:
                    raise RetryableFetchError(
                        f"HTTP {resp.status_code}: lawd_cd={lawd_cd}, deal_ym={deal_ym}, page_no={page_no}"
                    )

                resp.raise_for_status()

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

                # total>0인데 마지막 페이지 이전에 items가 비는 경우는 누락 위험 → 재시도
                if total > 0 and page < total_pages and not parsed:
                    raise RetryableFetchError(
                        f"empty items before last page: lawd_cd={lawd_cd}, deal_ym={deal_ym}, "
                        f"page_no={page}, total_pages={total_pages}"
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


# =============================
#  Pagination (pageNo 순회)
# =============================


def iter_month_pages(
    client: KrebClient,
    lawd_cd: str,
    deal_ym: str,
    start_page: int = 1,
):
    """
    한 (lawd_cd, deal_ym)에 대해 pageNo 기반으로 순회.

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
#  State (진행 상태 저장/조회)
# =============================


def load_state(fs, path: str) -> Dict[str, Any]:
    """
    fsspec file system에서 JSON state를 읽는다.
    state 구조 예:
    {
      "cursor": {
        "lawd_cd": "41135",
        "deal_ym": "201909",
        "page_no": 3
      },
      "done": false
    }
    """
    base: Dict[str, Any] = {
        "version": 2,
        "cursor": None,
        "done": False,
        "done_at": None,
        "last_success": None,
        "last_completed": None,
        "last_error": None,
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
    payload = json.dumps(state, ensure_ascii=False)
    if _is_s3_like_fs(fs):
        # S3/MinIO: 단일 객체 PUT(덮어쓰기) = 가장 단순한 커밋 단위
        with fs.open(path, "wt", encoding="utf-8") as f:
            f.write(payload)
        return

    tmp = path + ".tmp"
    with fs.open(tmp, "wt", encoding="utf-8") as f:
        f.write(payload)
    fs.move(tmp, path)


# =============================
#  CSV 저장 (S3/MinIO 또는 로컬)
# =============================


def write_page_csv(
    fs,
    base_path: str,
    lawd_cd: str,
    deal_ym: str,
    page_no: int,
    items: List[Dict[str, Any]],
) -> None:
    """
    응답 item 리스트를 CSV로 저장.
    경로 예:
      base_path/LAWD_CD=41135/DEAL_YM=201901/page=1.csv
    """
    if not items:
        return

    df = pd.DataFrame(items)

    dir_path = f"{base_path}/LAWD_CD={lawd_cd}/DEAL_YM={deal_ym}"
    file_path = f"{dir_path}/page={page_no}.csv"

    # 이미 있으면 스킵 → idempotent (단, 0바이트 같은 손상은 재작성)
    if fs.exists(file_path):
        try:
            info = fs.info(file_path)
            if int(info.get("size") or 0) > 0:
                return
        except Exception:
            return

    fs.mkdirs(dir_path, exist_ok=True)
    with fs.open(file_path, "wt", encoding="utf-8-sig") as f:
        df.to_csv(f, index=False)


def write_partition_success(
    fs,
    base_path: str,
    lawd_cd: str,
    deal_ym: str,
    manifest: Dict[str, Any],
) -> None:
    """(lawd_cd, deal_ym) 파티션 완료 마커를 기록한다."""
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


def build_s3_storage_options() -> Dict[str, Any]:
    """
    MinIO / S3용 fsspec storage_options 생성.
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY 환경변수 사용.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not (endpoint and access_key and secret_key):
        # 로컬 file:// 쓸 때 등은 그냥 빈 dict 리턴
        return {}

    use_ssl = endpoint.startswith("https://")

    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {
            "endpoint_url": endpoint,
        },
        "use_ssl": use_ssl,
    }


def build_s3_fs():
    """
    MinIO / S3용 s3fs.S3FileSystem 생성.
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY 환경변수 사용.
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
    """
    s3://bucket/key 형태의 URI를
    s3fs에서 사용하는 'bucket/key' 형태의 경로로 변환.
    """
    parsed = urlparse(uri)
    if parsed.scheme not in ("s3", "s3a"):
        raise ValueError(f"not an s3 uri: {uri}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")  # "/kreb_state.json" -> "kreb_state.json"
    return f"{bucket}/{key}" if key else bucket


def get_fs_and_path(uri: str, storage_options: Dict[str, Any]):
    """
    s3:// / file:// 모두 지원하는 fs + path 생성 helper.
    - s3://retrend-raw-data/path/to/file -> (S3FileSystem, "retrend-raw-data/path/to/file")
    - file:///tmp/test.txt -> (LocalFileSystem, "/tmp/test.txt")
    """
    parsed = urlparse(uri)

    # S3/MinIO
    if parsed.scheme in ("s3", "s3a"):
        fs = fsspec.filesystem("s3", **storage_options)
        # s3fs 에서는 'bucket/key' 형태로 path를 넘기는게 일반적
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")  # 앞의 "/" 제거
        path = f"{bucket}/{key}" if key else bucket
        return fs, path

    # 로컬 파일
    if parsed.scheme in ("file", ""):
        fs = fsspec.filesystem("file")
        # file:///tmp/a.csv -> /tmp/a.csv
        path = parsed.path
        return fs, path

    # 그 외 프로토콜은 기존 방식대로 url_to_fs 위임
    fs, path = fsspec.core.url_to_fs(uri, **storage_options)
    return fs, path


# =============================
#  Helper: 기간, LAWD 리스트
# =============================


def generate_last_10y_months(today: Optional[dt.date] = None) -> List[str]:
    """
    최근 10년간의 YYYYMM 리스트 (오래된 순).
    예) 오늘이 2025-12라면 2015-12 ~ 2025-12
    """
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
    """
    시군구 코드 CSV에서 LAWD_CD 목록을 읽는다.
    - s3:// 경로면 storage_options를 사용해 MinIO/S3에서 읽고
    - 그 외(file://, 로컬 경로)는 일반 read_csv로 읽는다.
    """
    read_kwargs: Dict[str, Any] = {"dtype": str}

    # s3:// 또는 s3a:// 인 경우에만 storage_options 적용
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
#  메인 엔트리: 하루 한 번 백필
# =============================


def run_backfill_once() -> None:
    """
    '하루에 한 번' 실행할 백필 엔트리포인트.

    환경변수:
      - KREB_SERVICE_KEY (필수)
      - KREB_BASE_URL (옵션, 기본값: 공식 아파트 매매 API)
      - KREB_DAILY_LIMIT (옵션, 기본 10000)
      - KREB_LAWD_CSV (필수, 시군구 코드 CSV, s3:// 또는 file:// 가능)
      - KREB_STATE_URI (옵션, state.json 저장 경로, 기본 file://./kreb_state.json)
      - KREB_OUTPUT_URI (필수, CSV 저장 base 경로, s3:// 또는 file://)
      - LOG_LEVEL (옵션, INFO/DEBUG 등)
    """

    # step 1. 환경변수 로드
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
    )
    log = logging.getLogger("kreb_etl_v2.backfill")

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

    state_uri = os.environ.get(
        "KREB_STATE_URI", "s3://retrend-raw-data/kreb_state.json"
    )

    output_uri = os.environ.get("KREB_OUTPUT_URI")
    if not output_uri:
        raise ValueError("환경변수 KREB_OUTPUT_URI 가 필요합니다. (CSV 저장 base 경로)")

    # step 2. 인스턴스 준비
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

    # MinIO / S3 옵션 + filesystem 생성
    storage_options = build_s3_storage_options()
    log.info("storage_options=%s", storage_options)

    s3_fs = build_s3_fs()

    # 1) state 파일용 FS / path
    if state_uri.startswith(("s3://", "s3a://")):
        if s3_fs is None:
            raise RuntimeError("S3 state_uri 를 쓰려면 MINIO_* 환경변수가 필요합니다.")
        fs_state = s3_fs
        state_path = s3_uri_to_path(state_uri)  # "retrend-raw-data/kreb_state.json"
    else:
        fs_state, state_path = fsspec.core.url_to_fs(state_uri)

    state = load_state(fs_state, state_path)
    ignore_done = os.environ.get("KREB_IGNORE_DONE", "0") == "1"
    if state.get("done") and not ignore_done:
        log.info("Backfill already done. Nothing to do.")
        return

    # 2) output 파일용 FS / base_path
    if output_uri.startswith(("s3://", "s3a://")):
        if s3_fs is None:
            raise RuntimeError("S3 output_uri 를 쓰려면 MINIO_* 환경변수가 필요합니다.")
        fs_out = s3_fs
        out_base_path = s3_uri_to_path(output_uri)  # "retrend-raw-data/bronze/..."
    else:
        fs_out, out_base_path = fsspec.core.url_to_fs(output_uri)

    # 3) LAWD CSV 읽기 (예전 방식처럼)
    lawd_list = load_lawd_codes_from_csv(lawd_csv, storage_options)
    ym_list = generate_last_10y_months()

    # --- cursor 위치 계산 ---
    cur = state.get("cursor")
    start_idx = 0
    if cur:
        try:
            lawd_idx = lawd_list.index(cur["lawd_cd"])
            ym_idx = ym_list.index(cur["deal_ym"])
            start_idx = ym_idx * len(lawd_list) + lawd_idx
        except ValueError:
            start_idx = 0  # csv가 바뀐 경우 등 → 처음부터 다시

    total_slots = len(lawd_list) * len(ym_list)
    log.info(
        "Start backfill: total_slots=%s, lawd_cnt=%s, ym_cnt=%s, start_idx=%s, daily_limit=%s",
        total_slots,
        len(lawd_list),
        len(ym_list),
        start_idx,
        daily_limit,
    )

    # --- 메인 루프: lawd × ym ---
    for idx in range(start_idx, total_slots):
        if quota.remaining <= 0:
            log.info("Quota exhausted: used=%s limit=%s", quota.used, quota.limit)
            break

        ym_idx, lawd_idx = divmod(idx, len(lawd_list))
        lawd_cd = lawd_list[lawd_idx]
        deal_ym = ym_list[ym_idx]

        # cursor 와 같으면 page_no 이어가기, 아니면 1부터 시작
        start_page = 1
        if cur and cur["lawd_cd"] == lawd_cd and cur["deal_ym"] == deal_ym:
            start_page = cur.get("page_no", 1)

        log.info(
            "Backfill month start: lawd_cd=%s, deal_ym=%s, start_page=%s, remaining=%s",
            lawd_cd,
            deal_ym,
            start_page,
            quota.remaining,
        )

        try:
            month_total_pages: Optional[int] = None
            last_page_no: Optional[int] = None

            for page_no, total_pages, items in iter_month_pages(
                client, lawd_cd, deal_ym, start_page
            ):
                # CSV 저장
                write_page_csv(
                    fs_out,
                    out_base_path,
                    lawd_cd,
                    deal_ym,
                    page_no,
                    items,
                )

                log.info(
                    "Fetched page: lawd_cd=%s, deal_ym=%s, page=%s/%s, items=%s, used=%s, remaining=%s",
                    lawd_cd,
                    deal_ym,
                    page_no,
                    total_pages,
                    len(items),
                    quota.used,
                    quota.remaining,
                )

                # 다음에 이어갈 cursor 업데이트 (next page)
                state["cursor"] = {
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "page_no": page_no + 1,
                }
                cur = state["cursor"]

                state["last_success"] = {
                    "ts": _now_iso(),
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "page_no": page_no,
                    "total_pages": total_pages,
                    "items_count": len(items),
                    "quota_used": quota.used,
                    "quota_remaining": quota.remaining,
                }
                state["last_error"] = None

                save_state(fs_state, state_path, state)

                month_total_pages = total_pages
                last_page_no = page_no

            # 월 단위 완료 처리: 마지막 페이지까지 도달해야 완료
            if (
                month_total_pages is not None
                and last_page_no is not None
                and last_page_no >= month_total_pages
            ):
                state["last_completed"] = {
                    "ts": _now_iso(),
                    "lawd_cd": lawd_cd,
                    "deal_ym": deal_ym,
                    "total_pages": month_total_pages,
                }

                write_partition_success(
                    fs_out,
                    out_base_path,
                    lawd_cd,
                    deal_ym,
                    {
                        "ts": _now_iso(),
                        "lawd_cd": lawd_cd,
                        "deal_ym": deal_ym,
                        "total_pages": month_total_pages,
                        "quota_used": quota.used,
                    },
                )

                # 다음 slot으로 cursor 이동 (완료된 월을 다시 호출하지 않도록)
                if idx >= total_slots - 1:
                    state["done"] = True
                    state["done_at"] = _now_iso()
                    state["cursor"] = None
                    cur = None
                    log.info("Backfill completed for all lawd×ym combinations.")
                else:
                    next_ym_idx, next_lawd_idx = divmod(idx + 1, len(lawd_list))
                    state["cursor"] = {
                        "lawd_cd": lawd_list[next_lawd_idx],
                        "deal_ym": ym_list[next_ym_idx],
                        "page_no": 1,
                    }
                    cur = state["cursor"]

                save_state(fs_state, state_path, state)

        except QuotaExceeded as e:
            log.warning(
                "Quota exceeded while fetching lawd_cd=%s, deal_ym=%s: %s",
                lawd_cd,
                deal_ym,
                e,
            )
            state["last_error"] = {
                "ts": _now_iso(),
                "type": type(e).__name__,
                "message": str(e),
                "lawd_cd": lawd_cd,
                "deal_ym": deal_ym,
                "page_no": start_page,
            }
            save_state(fs_state, state_path, state)
            break

        except Exception as e:
            state["last_error"] = {
                "ts": _now_iso(),
                "type": type(e).__name__,
                "message": str(e),
                "lawd_cd": lawd_cd,
                "deal_ym": deal_ym,
                "page_no": start_page,
            }
            save_state(fs_state, state_path, state)
            raise

    # 최종 state flush (이미 페이지 단위로 저장하지만, 방어적으로 1회 더)
    save_state(fs_state, state_path, state)


if __name__ == "__main__":
    run_backfill_once()
