WITH
{{SGG_MAP_CTE}},
latest AS (
  SELECT max(deal_month_date) AS d
  FROM iceberg.default.apt_trade_complex_monthly_median_ppsqm_10y_v1
),
complex_latest AS (
  SELECT
    aptseq,
    aptnm,
    sggcd,
    deal_month_date,
    median_ppsqm_krw AS complex_median_ppsqm_krw,
    trades AS complex_trades
  FROM iceberg.default.apt_trade_complex_monthly_median_ppsqm_10y_v1
  WHERE deal_month_date = (SELECT d FROM latest)
),
region_latest AS (
  SELECT
    sggcd,
    median_ppsqm_krw AS region_median_ppsqm_krw,
    trades AS region_trades
  FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1
  WHERE deal_month_date = (SELECT d FROM latest)
)
SELECT
  c.deal_month_date,
  c.sggcd,
  COALESCE(m.region_ko, c.sggcd) AS region_ko,
  c.aptseq,
  c.aptnm,
  c.complex_median_ppsqm_krw,
  r.region_median_ppsqm_krw,
  c.complex_median_ppsqm_krw / NULLIF(r.region_median_ppsqm_krw, 0) AS premium_ratio,
  c.complex_median_ppsqm_krw - r.region_median_ppsqm_krw AS premium_diff_krw,
  c.complex_trades
FROM complex_latest c
LEFT JOIN region_latest r ON c.sggcd = r.sggcd
LEFT JOIN sgg_map m ON c.sggcd = m.sggcd
WHERE c.complex_trades >= 3
  AND r.region_median_ppsqm_krw IS NOT NULL
