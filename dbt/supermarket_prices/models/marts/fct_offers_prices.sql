with offers as (
        select 
                update_date,
                supermarket_code,
                product_code,
                offer_detail,
                offer_detail_zh,
                upper(split_part(offer_detail,'/',1)) as tf_offer_detail,
        from {{ ref("int_offers") }} 
        ),
    parsed_offers as (
        select *,
                coalesce(
                        nullif(regexp_extract(tf_offer_detail, 'BUY (\d+)', 1),''),
                        nullif(regexp_extract(tf_offer_detail, '^(\d+) FOR', 1),''),
                        nullif(regexp_extract(tf_offer_detail, '^(\d+) OR MORE', 1),''),
                        nullif(regexp_extract(tf_offer_detail, 'FOR (\d)', 1),''),
                        nullif(regexp_extract(tf_offer_detail, ' (\d).*?X', 1),''),
                        nullif(regexp_extract(tf_offer_detail, ' X(\d)', 1),''),
                        nullif(regexp_extract(tf_offer_detail, ' (\d).*?PC', 1),''),
                        case 
                                when tf_offer_detail like '%BUY SECOND%' or tf_offer_detail like '%BUY 2ND%' then '2'
                                when tf_offer_detail like '%BUY THIRD%' or tf_offer_detail like '%BUY 3RD%' then '3'
                                when tf_offer_detail like '%BUY FORTH%' or tf_offer_detail like '%BUY 4TH%' then '4'
                                when tf_offer_detail like '%BUY FIFTH%' or tf_offer_detail like '%BUY 5TH%' then '5'
                                when tf_offer_detail like '%BUY SIXTH%' or tf_offer_detail like '%BUY 6TH%' then '6'
                                when tf_offer_detail like '%BUY SEVENTH%' or tf_offer_detail like '%BUY 7TH%' then '7'
                                when tf_offer_detail like '%BUY EIGTH%' or tf_offer_detail like '%BUY 8TH%' then '8'
                                when tf_offer_detail like '%BUY NINTH%' or tf_offer_detail like '%BUY 9TH%' then '9'
                                when tf_offer_detail like '%BUY TENTH%' or tf_offer_detail like '%BUY 10TH%' then '10'
                        end
                )::int
                as buy_quantity,
                nullif(regexp_extract(tf_offer_detail, 'ADD (\d+)', 1),'')::int as add_quantity,
                nullif(regexp_extract(tf_offer_detail, 'GET (\d+) FREE', 1),'')::int as free_quantity,
                nullif(regexp_extract(tf_offer_detail, '[FOR|AT] \$(\d+.\d+)', 1),'')::numeric as ttl_price,
                nullif(regexp_extract(tf_offer_detail, 'TO SAVE \$(\d+.\d+)', 1),'')::numeric as saved_amount,
                coalesce(
                        nullif(regexp_extract(tf_offer_detail, '(\d+\.\d+)\%.*?OFF', 1),''), 
                        nullif(regexp_extract(tf_offer_detail, '(\d+)\%.*?OFF', 1),''),
                
                )::numeric
                as total_discount,
                coalesce(
                        case when tf_offer_detail like '%2ND AT HALF PRICE%' then '50' end,
                        nullif(regexp_extract(tf_offer_detail, '(\d+)\% FOR', 1),''),
                        )::numeric
                as last_item_discount,
                coalesce(
                        case 
                        when tf_offer_detail like '%BUY SECOND%' or tf_offer_detail like '%BUY 2ND%'
                                or tf_offer_detail like '%BUY THIRD%' or tf_offer_detail like '%BUY 3RD%'
                                or tf_offer_detail like '%BUY FORTH%' or tf_offer_detail like '%BUY 4TH%'
                                or tf_offer_detail like '%BUY FIFTH%' or tf_offer_detail like '%BUY 5TH%'
                                or tf_offer_detail like '%BUY SIXTH%' or tf_offer_detail like '%BUY 6TH%'
                                or tf_offer_detail like '%BUY SEVENTH%' or tf_offer_detail like '%BUY 7TH%'
                                or tf_offer_detail like '%BUY EIGTH%' or tf_offer_detail like '%BUY 8TH%'
                                or tf_offer_detail like '%BUY NINTH%' or tf_offer_detail like '%BUY 9TH%'
                                or tf_offer_detail like '%BUY TENTH%' or tf_offer_detail like '%BUY 10TH%'
                        then
                                nullif(regexp_extract(tf_offer_detail, '[FOR|AT] \$(\d+.\d+)', 1),'') end
                        )::numeric
                as last_item_price,
        from offers
        ),
        result as (
                select 
                p.product_code,
                p.update_date,
                p.supermarket_code,
                p.price,
                o.offer_detail,
                o.offer_detail_zh,
                case 
                    when last_item_price is not null and buy_quantity is not null then (p.price * (o.buy_quantity - 1) + o.last_item_price) / (o.buy_quantity)
                    when total_discount is not null then p.price * (1 - o.total_discount/100)
                    when last_item_discount is not null and buy_quantity is not null then (p.price * (o.buy_quantity - 1) + p.price * (1 - o.last_item_discount/100)) / (o.buy_quantity + 1)
                    when add_quantity is not null and free_quantity is not null then p.price * add_quantity/ (add_quantity + free_quantity)
                    when buy_quantity is not null and free_quantity is not null then p.price * (buy_quantity) / (buy_quantity + free_quantity)
                    when buy_quantity is not null and ttl_price is not null then ttl_price / buy_quantity
                    when buy_quantity is not null and saved_amount is not null then (buy_quantity * price - saved_amount)/buy_quantity
                end as discounted_price
            from {{ ref("int_prices") }} p 
            left join parsed_offers o on 
                p.product_code = o.product_code 
                and p.update_date = o.update_date 
                and p.supermarket_code = o.supermarket_code
    )
select  *,
        (1-discounted_price/price) * 100 as discount_off
from result 
