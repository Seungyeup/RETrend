SELECT
  deal_month_date,
  SUM(trades) AS trades
FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1
GROUP BY deal_month_date
