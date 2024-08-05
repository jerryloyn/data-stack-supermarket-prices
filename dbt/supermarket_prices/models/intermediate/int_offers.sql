select distinct 
    product_code,
    update_date,
    unnest(offers)['supermarketCode'] as supermarket_code,
    unnest(offers)['en'] as offer_detail,
    unnest(offers)['zh-Hant'] as offer_detail_zh,
from {{ ref("stg_product_prices") }}