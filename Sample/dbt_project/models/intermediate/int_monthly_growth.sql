with indicators as (
    select * from {{ ref('int_monthly_indicators') }}
),

lagged_indicators as (
    select
        record_month,
        country,
        avg_oil_price_usd,
        avg_interest_rate_pct,
        lag(avg_oil_price_usd) over (partition by country order by record_month) as prev_oil_price_usd,
        lag(avg_interest_rate_pct) over (partition by country order by record_month) as prev_interest_rate_pct
    from indicators
)

select
    record_month,
    country,
    avg_oil_price_usd,
    avg_interest_rate_pct,
    case 
        when prev_oil_price_usd > 0 
        then (avg_oil_price_usd - prev_oil_price_usd) / prev_oil_price_usd
        else null 
    end as oil_price_mom_growth,
    case 
        when prev_interest_rate_pct > 0 
        then (avg_interest_rate_pct - prev_interest_rate_pct) / prev_interest_rate_pct
        else null 
    end as interest_rate_mom_growth
from lagged_indicators
