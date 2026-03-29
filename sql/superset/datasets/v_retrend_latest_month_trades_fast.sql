WITH monthly AS (
  SELECT
    deal_month_date,
    SUM(trades) AS trades
  FROM iceberg.default.apt_trade_region_monthly_ppsqm_stats_15y_v1
  GROUP BY deal_month_date
)
SELECT
  deal_month_date,
  trades
FROM monthly
WHERE deal_month_date = (SELECT max(deal_month_date) FROM monthly)
