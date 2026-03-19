with oil as (
    select
        -- Adjust date_trunc depending on your target data warehouse (e.g. Postgres vs Snowflake vs BigQuery)
        date_trunc('month', record_date) as record_month,
        country,
        avg(oil_price_usd) as avg_oil_price_usd
    from {{ ref('stg_oil_prices') }}
    group by 1, 2
),

interest as (
    select
        date_trunc('month', record_date) as record_month,
        country,
        avg(interest_rate_pct) as avg_interest_rate_pct
    from {{ ref('stg_interest_rates') }}
    group by 1, 2
)

select
    coalesce(o.record_month, i.record_month) as record_month,
    coalesce(o.country, i.country) as country,
    o.avg_oil_price_usd,
    i.avg_interest_rate_pct
from oil o
full outer join interest i
    on o.record_month = i.record_month
    and o.country = i.country
