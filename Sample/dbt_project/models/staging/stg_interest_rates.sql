with raw_data as (
    select * from {{ source('market_data', 'global_market_ai_dataset') }}
)

select
    cast("Date" as date) as record_date,
    "Country" as country,
    avg(cast("Interest_Rate" as decimal(10,4))) as interest_rate_pct
from raw_data
group by 1, 2
