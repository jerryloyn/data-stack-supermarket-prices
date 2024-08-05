select  
    f.update_date,
    d.day_of_week_name as weekday,
    f.product_code,
    p.brand_name,
    p.product_name,
    p.product_cat_1,
    p.product_cat_2,
    p.product_cat_3,
    f.supermarket_code,
    s.supermarket_name,
    f.price,
    f.discounted_price,
    f.discount_off,
    f.offer_detail,
from {{ ref("fct_offers_prices") }} f
left join {{ ref("dim_dates") }} d on f.update_date = d.date_day
left join {{ ref("dim_products") }} p on f.product_code = p.product_code
left join {{ ref("dim_supermarkets") }} s on f.supermarket_code = s.supermarket_code


