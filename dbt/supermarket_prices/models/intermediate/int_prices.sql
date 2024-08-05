select distinct 
    product_code,
    update_date,
    unnest(prices)['supermarketCode'] as supermarket_code,
    unnest(prices)['price']::numeric as price,
from {{ ref("stg_product_prices") }}