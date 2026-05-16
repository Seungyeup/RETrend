SELECT *
FROM (
{{SQL_V_RETREND_REGION_DECISION_MONTHLY_FAST}}
) t
WHERE deal_month_date = (
  SELECT max(deal_month_date)
  FROM (
{{SQL_V_RETREND_REGION_DECISION_MONTHLY_FAST}}
  ) x
)
  AND trades >= 5
