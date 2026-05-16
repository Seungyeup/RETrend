import duckdb
con = duckdb.connect()

# Iceberg 확장 설치(최초 1회)
con.execute("INSTALL iceberg;")
# 매번 세션에 로드
con.execute("LOAD iceberg;")