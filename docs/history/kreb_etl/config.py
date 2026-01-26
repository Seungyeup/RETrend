from __future__ import annotations
import os
from dataclasses import dataclass

""" 
S3 Configuration 
"""
@dataclass
class S3Config:
    endpoint: str | None
    access_key: str | None
    secret_key: str | None

    @property
    def storage_options(self) -> dict:
        return {
            "client_kwargs": {
                "endpoint_url": self.endpoint,
            },
            "key": self.access_key,
            "secret": self.secret_key,
        }

"""
KREB Configuration
"""
@dataclass
class KREBConfig:
    api_bases: list[str]
    api_path: str
    service_key: str | None
    lawd_codes_s3: str | None
    lawd_codes_env: list[str]
    shigungu_s3: str
    start_ym: str
    end_ym: str
    sleep_sec: float
    retries: int
    backoff: float
    limit_codes: int
    limit_months: int
    limit_pages: int
    num_rows: int
    dry_run_sample: bool
    no_write: bool
    out_prefix: str
    requests_headers: dict


"""
Load S3 Configuration
"""
def load_s3_config() -> S3Config:
    return S3Config(
        endpoint=os.environ.get("MINIO_ENDPOINT"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
    )


"""
Load KREB Configuration
"""
def load_kreb_config() -> KREBConfig:
    default_api_bases = ["https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev"]
    # api_bases = [
    #     b.strip()
    #     for b in os.environ.get("KREB_API_BASES", "").split(",")
    #     if b.strip()
    # ] or default_api_bases
    api_bases = default_api_bases

    lawd_codes_env = [
        c.strip()
        for c in os.environ.get("KREB_LAWD_CODES", "").split(",")
        if c.strip()
    ]

    headers = {
        "User-Agent": os.environ.get(
            "KREB_UA",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36",
        ),
        "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
    }

    return KREBConfig(
        api_bases=api_bases,
        api_path=os.environ.get("KREB_API_PATH", "getRTMSDataSvcAptTradeDev"),
        service_key=os.environ.get("KREB_SERVICE_KEY") or os.environ.get("SERVICE_KEY"),
        lawd_codes_s3=os.environ.get("KREB_LAWD_CODES_S3", "") or None,
        lawd_codes_env=lawd_codes_env,
        shigungu_s3=os.environ.get("KREB_SHIGUNGU_S3", "s3://retrend-raw-data/shigungu_list.csv"),
        start_ym=os.environ.get("START_YM", ""),
        end_ym=os.environ.get("END_YM", ""),
        sleep_sec=float(os.environ.get("KREB_SLEEP", "0.5")),
        retries=int(os.environ.get("KREB_RETRIES", "3") or 3),
        backoff=float(os.environ.get("KREB_BACKOFF", "1.0")),
        limit_codes=int(os.environ.get("KREB_LIMIT_CODES", "0") or 0),
        limit_months=int(os.environ.get("KREB_LIMIT_MONTHS", "0") or 0),
        limit_pages=int(os.environ.get("KREB_LIMIT_PAGES", "0") or 0),
        num_rows=int(os.environ.get("KREB_NUM_ROWS", "100") or 100),
        dry_run_sample=os.environ.get("DRY_RUN_SAMPLE", "0") == "1",
        no_write=os.environ.get("NO_WRITE", "0") == "1",
        out_prefix=os.environ.get("KREB_BRONZE_S3", "s3://retrend-raw-data/bronze/kreb/apt_trade/").rstrip("/"),
        requests_headers=headers,
    )