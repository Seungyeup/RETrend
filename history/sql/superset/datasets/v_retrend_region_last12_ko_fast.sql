WITH
{{SGG_MAP_CTE}}
SELECT
  r.sggcd,
  COALESCE(m.region_ko, r.sggcd) AS region_ko,
  r.median_ppsqm_krw_12m,
  r.trades_12m
FROM iceberg.default.apt_trade_region_median_ppsqm_last12m_v1 r
LEFT JOIN sgg_map m ON r.sggcd = m.sggcd
