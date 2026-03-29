WITH
{{SGG_MAP_CTE}},
complex_agg AS (
  SELECT
    aptseq,
    aptnm,
    sggcd,
    SUM(median_ppsqm_krw_12m * trades_12m) / NULLIF(SUM(trades_12m), 0) AS complex_weighted_ppsqm_12m,
    SUM(trades_12m) AS complex_trades_12m
  FROM iceberg.default.apt_trade_complex_area_bucket_median_ppsqm_12m_v1
  GROUP BY 1, 2, 3
),
region_12m AS (
  SELECT
    sggcd,
    median_ppsqm_krw_12m AS region_weighted_ppsqm_12m,
    trades_12m AS region_trades_12m
  FROM iceberg.default.apt_trade_region_median_ppsqm_last12m_v1
)
SELECT
  c.sggcd,
  COALESCE(m.region_ko, c.sggcd) AS region_ko,
  c.aptseq,
  c.aptnm,
  c.complex_weighted_ppsqm_12m,
  r.region_weighted_ppsqm_12m,
  c.complex_weighted_ppsqm_12m / NULLIF(r.region_weighted_ppsqm_12m, 0) AS gap_ratio_12m,
  c.complex_weighted_ppsqm_12m - r.region_weighted_ppsqm_12m AS gap_diff_krw_12m,
  c.complex_trades_12m
FROM complex_agg c
LEFT JOIN region_12m r ON c.sggcd = r.sggcd
LEFT JOIN sgg_map m ON c.sggcd = m.sggcd
WHERE c.complex_trades_12m >= 10
  AND r.region_weighted_ppsqm_12m IS NOT NULL
