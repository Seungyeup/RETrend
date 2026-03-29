WITH
{{SGG_MAP_CTE}},
base AS (
  SELECT
    sggcd,
    deal_month_date,
    deal_year,
    deal_month,
    median_ppsqm_krw,
    trades
  FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1
),
feat AS (
  SELECT
    b.*,
    (b.median_ppsqm_krw / NULLIF(LAG(b.median_ppsqm_krw) OVER (PARTITION BY b.sggcd ORDER BY b.deal_month_date), 0) - 1) AS mom_pct,
    (b.median_ppsqm_krw / NULLIF(LAG(b.median_ppsqm_krw, 12) OVER (PARTITION BY b.sggcd ORDER BY b.deal_month_date), 0) - 1) AS yoy_pct,
    AVG(b.median_ppsqm_krw) OVER (PARTITION BY b.sggcd ORDER BY b.deal_month_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS ma24,
    STDDEV_SAMP(b.median_ppsqm_krw) OVER (PARTITION BY b.sggcd ORDER BY b.deal_month_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS std24,
    AVG(b.trades) OVER (PARTITION BY b.sggcd ORDER BY b.deal_month_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS trades_ma12
  FROM base b
),
score_base AS (
  SELECT
    f.*,
    AVG(f.mom_pct) OVER (PARTITION BY f.sggcd ORDER BY f.deal_month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mom3_avg,
    STDDEV_SAMP(f.mom_pct) OVER (PARTITION BY f.sggcd ORDER BY f.deal_month_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS mom_vol_12m
  FROM feat f
)
SELECT
  s.deal_month_date,
  s.deal_year,
  s.deal_month,
  s.sggcd,
  COALESCE(m.region_ko, s.sggcd) AS region_ko,
  s.median_ppsqm_krw,
  s.trades,
  s.mom_pct,
  s.yoy_pct,
  s.ma24,
  s.std24,
  s.trades_ma12,
  s.mom3_avg,
  s.mom_vol_12m,
  (s.median_ppsqm_krw - s.ma24) / NULLIF(s.ma24, 0) AS valuation_gap_pct,
  (s.median_ppsqm_krw - s.ma24) / NULLIF(s.std24, 0) AS valuation_z,
  s.trades / NULLIF(s.trades_ma12, 0) AS liquidity_ratio,
  (
    0.35 * (LEAST(GREATEST(COALESCE(s.yoy_pct, 0), -0.30), 0.30) / 0.30)
    + 0.25 * (LEAST(GREATEST(COALESCE(s.mom3_avg, 0), -0.15), 0.15) / 0.15)
    + 0.20 * (LEAST(GREATEST(COALESCE(s.trades / NULLIF(s.trades_ma12, 0) - 1, 0), -0.70), 0.70) / 0.70)
    - 0.20 * (LEAST(GREATEST(COALESCE((s.median_ppsqm_krw - s.ma24) / NULLIF(s.std24, 0), 0), -2.0), 2.0) / 2.0)
  ) AS signal_score,
  CASE
    WHEN s.ma24 IS NULL OR s.std24 IS NULL THEN '판단보류(데이터부족)'
    WHEN s.median_ppsqm_krw < s.ma24 - s.std24 THEN '저평가권'
    WHEN s.median_ppsqm_krw > s.ma24 + s.std24 THEN '고평가권'
    ELSE '적정권'
  END AS valuation_state,
  CASE
    WHEN (
      0.35 * (LEAST(GREATEST(COALESCE(s.yoy_pct, 0), -0.30), 0.30) / 0.30)
      + 0.25 * (LEAST(GREATEST(COALESCE(s.mom3_avg, 0), -0.15), 0.15) / 0.15)
      + 0.20 * (LEAST(GREATEST(COALESCE(s.trades / NULLIF(s.trades_ma12, 0) - 1, 0), -0.70), 0.70) / 0.70)
      - 0.20 * (LEAST(GREATEST(COALESCE((s.median_ppsqm_krw - s.ma24) / NULLIF(s.std24, 0), 0), -2.0), 2.0) / 2.0)
    ) >= 0.35 THEN '상승우세'
    WHEN (
      0.35 * (LEAST(GREATEST(COALESCE(s.yoy_pct, 0), -0.30), 0.30) / 0.30)
      + 0.25 * (LEAST(GREATEST(COALESCE(s.mom3_avg, 0), -0.15), 0.15) / 0.15)
      + 0.20 * (LEAST(GREATEST(COALESCE(s.trades / NULLIF(s.trades_ma12, 0) - 1, 0), -0.70), 0.70) / 0.70)
      - 0.20 * (LEAST(GREATEST(COALESCE((s.median_ppsqm_krw - s.ma24) / NULLIF(s.std24, 0), 0), -2.0), 2.0) / 2.0)
    ) <= -0.35 THEN '조정우세'
    ELSE '중립'
  END AS market_phase
FROM score_base s
LEFT JOIN sgg_map m ON s.sggcd = m.sggcd
