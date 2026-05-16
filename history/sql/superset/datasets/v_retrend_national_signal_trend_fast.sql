SELECT
  deal_month_date,
  approx_percentile(signal_score, 0.5) AS signal_score_median,
  AVG(signal_score) AS signal_score_avg,
  AVG(valuation_gap_pct) AS valuation_gap_avg,
  AVG(liquidity_ratio) AS liquidity_ratio_avg,
  AVG(mom3_avg) AS mom3_avg,
  AVG(yoy_pct) AS yoy_avg
FROM (
{{SQL_V_RETREND_REGION_DECISION_MONTHLY_FAST}}
) t
WHERE trades >= 5
GROUP BY deal_month_date
