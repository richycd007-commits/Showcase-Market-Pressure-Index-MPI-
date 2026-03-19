with raw_data as (
    select * from {{ source('market_data', 'global_market_ai_dataset') }}
)

select
    cast("Date" as date) as record_date,
    cast("Closing_Price" as decimal(10,2)) as oil_price_usd,
    "Country" as country
from raw_data
where "Asset_Name" = 'Oil'
