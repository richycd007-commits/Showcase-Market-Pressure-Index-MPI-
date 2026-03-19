with monthly_growth as (
    select * from {{ ref('int_monthly_growth') }}
)

select
    record_month,
    country,
    avg_oil_price_usd,
    avg_interest_rate_pct,
    oil_price_mom_growth,
    interest_rate_mom_growth,
    case
        -- Define severe headwinds: Oil price up significantly (>5%) AND Interest rates up significantly (>2%)
        when oil_price_mom_growth > 0.05 and interest_rate_mom_growth > 0.02 then 'Severe Headwinds'
        -- Moderate headwinds: Meaningful increase in either cost of fuel or cost of capital
        when oil_price_mom_growth > 0.03 or interest_rate_mom_growth > 0.01 then 'Moderate Headwinds'
        -- Favorable: Oil dropped AND Interest rates dropped
        when oil_price_mom_growth < -0.02 and interest_rate_mom_growth < -0.01 then 'Favorable Conditions'
        -- Otherwise neutral
        else 'Neutral/Baseline'
    end as construction_headwind_status,
    case
        when oil_price_mom_growth > 0 then 'Fuel Costs Increasing'
        else 'Fuel Costs Easing'
    end as fuel_cost_trend,
    case
        when interest_rate_mom_growth > 0 then 'Financing Tightening'
        else 'Financing Easing'
    end as financing_trend
from monthly_growth
