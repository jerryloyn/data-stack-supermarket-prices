with source_data as (
    select  
        json_extract_string(code, '$') as product_code,
        brand,
        name as product_name,
        cat1Name as product_cat_1,
        cat2Name as product_cat_2,
        cat3Name as product_cat_3,
        prices,
        offers,
        cast(strptime(parse_path(filename)[-1], '%Y%m%d.json') as DATE) as update_date
    from read_json(
            's3://prices-bucket/*.json',
            format = 'array',
            filename = true
        )
)
select *
from source_data