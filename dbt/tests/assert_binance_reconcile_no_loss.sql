{% set day = var('reconcile_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat()) %}
SELECT symbol, hour_utc, ex_n, ours_n, round(ratio_pct, 2) AS ratio_pct
FROM {{ ref('int_binance_reconcile_hourly') }}
WHERE toDate(hour_utc) = toDate('{{ day }}') AND ex_n > 0 AND ratio_pct < 99
