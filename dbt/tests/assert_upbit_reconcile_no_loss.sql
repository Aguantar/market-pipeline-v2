-- 유실 게이트 (v1 docs/13·17): 대상일(reconcile_date, 기본 어제 UTC)에 거래소 거래량 대비 99% 미만 셀이 없어야 한다. 반환 행 = 실패 셀.
{% set day = var('reconcile_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat()) %}
SELECT market, hour_utc, ch_vol, candle_vol, round(ratio_pct, 2) AS ratio_pct, ch_n
FROM {{ ref('int_upbit_reconcile_hourly') }}
WHERE toDate(hour_utc) = toDate('{{ day }}') AND candle_vol > 0 AND ratio_pct < 99
