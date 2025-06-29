import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# 경로 설정
csv_path = os.path.expanduser('~/nfs_mount/trade_history_13337.csv')
parquet_root = '~/nfs_mount/parquet'

# Parquet 저장 디렉토리 생성
os.makedirs(parquet_root, exist_ok=True)

# CSV 로드 (모든 컬럼 그대로 읽기)
df = pd.read_csv(csv_path)

# 날짜 컬럼 생성
df['date'] = pd.to_datetime(
    df['tradeYear'].astype(str) + '-' +
    df['tradeMonth'].astype(str).str.zfill(2) + '-' +
    df['tradeDate'].astype(str).str.zfill(2)
)

# Parquet 파일로 쓰기 (모든 컬럼, date 포함)
table = pa.Table.from_pandas(df)
pq.write_table(
    table,
    os.path.join(parquet_root, 'trade_history.parquet'),
    compression='snappy'
)

print("✅ Parquet 변환 완료:", os.path.join(parquet_root, 'trade_history.parquet'))
