# Data Engineering Project: Supermarket Product Prices
## Overview
This project aims to collect product prices of Hong Kong supermarkets via an API, store the raw data in `MinIO`, perform data modeling using `dbt`, transform the data with `DuckDB`, and visualize the results using `Streamlit`. The entire workflow is orchestrated by `Airflow`.

The dashboard is also published on streamlit [here](https://hk-supermarket-prices.streamlit.app/)

## Tech Stack
1. API Data Collection:
    - Use an API to fetch daily product prices of supermarkets. (see details [here](https://data.gov.hk/en-data/dataset/cc-pricewatch-pricewatch))
    - Store the raw data in `MinIO` (an object storage service).
1. Data Modeling with dbt:
    - `dbt` (Data Build Tool) is a SQL-based data transformation tool.
    - the raw JSON from the API is normalized to facts and dimensions tables, and then denormalized to a One Big Table for `Streamlit` dashboard
1. Data Transformation with DuckDB:
    - `dbt build` command trigger the data transformations, aggregations, and calculations in `DuckDB`.
1. Data Visualization with Streamlit:
    - Streamlit is a Python library for creating interactive web applications.
    - Our dashboard shows the best grocery deal and what product category/brand/supermarkets offer more discounts.

## Getting Started
1. Setting Up Containers:
    - Clone this repository.
    - Make sure you have Docker installed.
    - Navigate to the project directory.
    - Run `make up` to spin up the containers:
1. Check the DAG run
    -  [Airflow UI](http:localhost:8000) with username and password are both airflow.
    ![alt airflow-ui](https://github.com/jerryloyn/data-stack-supermarket-prices/blob/master/assets/airflow_screenshot.png?raw=true)
1. Run `make run-duckdb` to open DuckDB cli for db query (Optional)
1. Run `make dbt-doc` to generate dbt docs (Optional)
    - [dbt UI](http:localhost:8080)
    ![alt dbt-lineage](https://github.com/jerryloyn/data-stack-supermarket-prices/blob/master/assets/dbt_lineage_screenshot.png?raw=true)
1. View Streamlit dashboard
    - [Streamlit Dashboard](http://localhost:8501)
    ![alt streamlit](https://github.com/jerryloyn/data-stack-supermarket-prices/blob/master/assets/streamlit_screenshot.png?raw=true)
1. Run `make down` to stop containers and removes containers after you are done

## Contributing
Feel free to contribute to this project by adding new features, improving documentation, or optimizing the workflow. Pull requests are welcome!