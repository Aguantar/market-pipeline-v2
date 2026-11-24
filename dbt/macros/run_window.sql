{#- 증분 모델의 기준 시각 (v1 docs/40 ⑩). 평소 now(), 백필은 --vars '{"run_date": "YYYY-MM-DD"}' (Airflow 가 ds 를 넘긴다). -#}
{%- macro run_anchor() -%}
    {%- set rd = var('run_date', none) -%}
    {%- if rd -%}toDateTime('{{ rd }} 00:00:00', 'UTC'){%- else -%}now('UTC'){%- endif -%}
{%- endmacro -%}

{#- 1분 집계 증분 창: 백필이면 그 날 하루, 평소면 어제 00:00 UTC 부터 지금까지. insert_overwrite 가 그 날짜 파티션을 통째로 바꾼다. -#}
{%- macro minute_window(col) -%}
    {%- set rd = var('run_date', none) -%}
    {%- if rd -%}
        {{ col }} >= toDateTime('{{ rd }} 00:00:00', 'UTC') AND {{ col }} < toDateTime('{{ rd }} 00:00:00', 'UTC') + INTERVAL 1 DAY
    {%- else -%}
        {{ col }} >= toStartOfDay(now('UTC')) - INTERVAL 1 DAY
    {%- endif -%}
{%- endmacro -%}
