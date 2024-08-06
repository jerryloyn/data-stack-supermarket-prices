import streamlit as st
import altair as alt
import duckdb
import os

con = duckdb.connect(database='app/data/dbt.duckdb', read_only=True)

st.set_page_config('Best Grocery Offers in HK', "🛒" ,initial_sidebar_state='collapsed', layout="wide")

st.title('🛒 Best Grocery Offers in HK')

st.subheader('Best Deals')

cols_a = st.columns(6)

with cols_a[0]:
    deal_or_all = st.selectbox('All Goods or New Deals', ["All Goods", "New Deals"])

with cols_a[1]: 
    cat_1_df = con.execute("""
        SELECT 'All'
                                
        union all
                                
        SELECT 
            DISTINCT product_cat_1
        FROM dim_products 
        ORDER BY 1
    """).df()
    cat_1 = st.selectbox('Product Cat 1', cat_1_df)

with cols_a[2]:
    cat_2_df = con.execute(f"""
        SELECT 'All'
                                
        union all
                                
        (SELECT 
            DISTINCT product_cat_2
        FROM dim_products 
        WHERE 1=1
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
        ORDER BY 1)
    """, [cat_1]).fetchdf()
    cat_2 = st.selectbox('Product Cat 2', cat_2_df)

with cols_a[3]:
    cat_3_df = con.execute(f"""
        SELECT 'All'
                                
        union all
                                
        (SELECT 
            DISTINCT product_cat_3
        FROM dim_products 
        WHERE 1=1
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
            AND (product_cat_2 = ? {"OR 1=1" if cat_2=='All' else ""})
        ORDER BY 1)
    """, [cat_1, cat_2]).fetchdf()
    cat_3 = st.selectbox('Product Cat 3', cat_3_df)

with cols_a[4]:
    brand_df = con.execute(f"""
        SELECT 'All'
                                
        union all
                                
        (SELECT 
            DISTINCT brand_name
        FROM dim_products 
        WHERE 1=1
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
            AND (product_cat_2 = ? {"OR 1=1" if cat_2=='All' else ""})
            AND (product_cat_3 = ? {"OR 1=1" if cat_3=='All' else ""})
        ORDER BY 1)
    """, [cat_1, cat_2, cat_3]).fetchdf()
    brand = st.selectbox('Brand', brand_df)

with cols_a[5]:
    product_df = con.execute(f"""
        SELECT 'All'
                                
        union all
                                
        (SELECT 
            DISTINCT product_name
        FROM dim_products 
        WHERE 1=1
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
            AND (product_cat_2 = ? {"OR 1=1" if cat_2=='All' else ""})
            AND (product_cat_3 = ? {"OR 1=1" if cat_3=='All' else ""})
            AND (brand_name = ? {"OR 1=1" if brand=='All' else ""})
        ORDER BY 1)
    """, [cat_1, cat_2, cat_3, brand]).fetchdf()
    product = st.selectbox('Product Name', product_df)

deal_or_all_filter = """
    inner join pre_day p on l.product_code = p.product_code and l.supermarket_name = p.supermarket_name and l.offer_detail is not null and p.offer_detail is null
"""
query= f"""
    with 
        last_day as(
        select  *
        from obt_offers_prices
        where update_date = (select max(update_date) from obt_offers_prices)
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
            AND (product_cat_2 = ? {"OR 1=1" if cat_2=='All' else ""})
            AND (product_cat_3 = ? {"OR 1=1" if cat_3=='All' else ""})
            AND (brand_name = ? {"OR 1=1" if brand=='All' else ""})
            AND (product_name = ? {"OR 1=1" if product=='All' else ""})
    ),
        pre_day as(
        select *
        from obt_offers_prices
        where update_date = (select max(update_date) from obt_offers_prices) - INTERVAL 1 DAY
    ), result as (
        select  l.product_name, l.brand_name ,l.offer_detail, l.discount_off, l.discounted_price, l.price, l.supermarket_name,l.product_cat_1
        from last_day l
        {deal_or_all_filter if deal_or_all=="New Deals" else "" }
        order by l.discount_off desc, l.product_cat_1, l.product_name, l.offer_detail, l.discounted_price, l.price
    )
    select * from result;
"""

main_table_head = con.execute(query, [cat_1, cat_2, cat_3, brand, product]).df()
main_table_head.set_index(main_table_head.columns[0], inplace=True)
main_table_head = main_table_head.style.format(
        {
            "discount_off": "{:.1f}%".format,
            "price": "${:.2f}".format,
            "discounted_price": "${:.2f}".format,
            }
        )

main_table_count = con.execute(query.replace("select * from result","select count(*) from result"), [cat_1, cat_2, cat_3, brand, product]).fetchone()[0]


last_update_date = con.execute("select max(update_date) from obt_offers_prices;").fetchone()[0]
st.write('Last Update Date ', last_update_date,) 
st.write('Total number of rows: ', main_table_count) 
st.write('First 100 rows: ')
st.dataframe(main_table_head, use_container_width=True,)

st.subheader('% Products with Offers vs Avg Discount Off')

circle_color = st.selectbox('Select Category Breakdown', ["product_cat_1", "product_cat_2", "product_cat_3", "brand_name","supermarket_name"])

product_offers_df = con.execute(f"""
 with 
    last_day as(
        select  *
        from obt_offers_prices
        where update_date = (select max(update_date) from obt_offers_prices)
            AND (product_cat_1 = ? {"OR 1=1" if cat_1=='All' else ""})
            AND (product_cat_2 = ? {"OR 1=1" if cat_2=='All' else ""})
            AND (product_cat_3 = ? {"OR 1=1" if cat_3=='All' else ""})
            AND (brand_name = ? {"OR 1=1" if brand=='All' else ""})
            AND (product_name = ? {"OR 1=1" if product=='All' else ""})
    ), result as (
        select 
                l.{circle_color},
                count(distinct case when l.offer_detail is not null then l.product_code end)/count(distinct l.product_code)*100 as offer_perc,
                avg(distinct case when l.offer_detail is not null then l.discount_off end) avg_discount_off,
                count(distinct l.product_code) as product_count,
        from last_day l
        group by 1
    )
    select * from result;
    """,  [cat_1, cat_2, cat_3, brand, product]).df()

chart = alt.Chart(product_offers_df).mark_circle().encode(
    x = alt.X('offer_perc:Q',).scale(zero=False),
    y = alt.Y('avg_discount_off:Q',).scale(zero=False),
    color = circle_color,
    size=alt.Size('product_count:Q',).scale(zero=False),
).interactive()

st.altair_chart(chart, theme="streamlit", use_container_width=True)

st.divider()
st.write('Product prices and discount offers gathered from Online Price Watch is owned by Consumer Council. More info @ https://data.gov.hk/en-data/dataset/cc-pricewatch-pricewatch')