{{ config(order_by='(market, hour_utc)') }}
-- 거래소 시간봉 거래량(진실값) 대비 우리 체결 거래량. 기준 시각은 체결 시각 (v1 docs/08·17).
WITH candles AS (SELECT market, hour_utc, volume AS candle_vol, amount AS candle_amt FROM {{ source('reference', 'upbit_hourly_candles') }} FINAL),
bounds AS (SELECT min(hour_utc) AS h_min, max(hour_utc) + INTERVAL 1 HOUR AS h_max FROM candles),
trades AS (
    SELECT market, hour_utc, sum(volume) AS ch_vol, count() AS ch_n
    FROM {{ ref('stg_upbit_trades') }}
    WHERE trade_ts >= (SELECT h_min FROM bounds) AND trade_ts < (SELECT h_max FROM bounds)
    GROUP BY market, hour_utc
)
SELECT c.market, c.hour_utc, ifNull(t.ch_vol, 0) AS ch_vol, ifNull(t.ch_n, 0) AS ch_n, c.candle_vol, c.candle_amt,
       if(c.candle_vol > 0, 100 * ifNull(t.ch_vol, 0) / c.candle_vol, NULL) AS ratio_pct, now() AS reconciled_at
FROM candles c LEFT JOIN trades t ON c.market = t.market AND c.hour_utc = t.hour_utc
