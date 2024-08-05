select distinct 
    product_code,
    brand['en'] as brand_name,
    brand['zh-Hant'] as brand_name_zh,
    product_name['en'] as product_name,
    product_name['zh-Hant'] as product_name_zh,
    product_cat_1['en'] as product_cat_1,
    product_cat_1['zh-Hant'] as product_cat_1_zh,
    product_cat_2['en'] as product_cat_2,
    product_cat_2['zh-Hant'] as product_cat_2_zh,
    product_cat_2['en'] as product_cat_3,
    product_cat_3['zh-Hant'] as product_cat_3_zh,
from {{ ref("stg_product_prices") }}