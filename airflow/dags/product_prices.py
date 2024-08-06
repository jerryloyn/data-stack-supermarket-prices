import os
import json
import requests
import pytz
import pandas as pd
import duckdb
from minio import Minio
from io import BytesIO
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
   "project_dir": "/usr/app/dbt",
   "profiles_dir": "/usr/app/dbt",
   "target": "dev",
   "profile": "supermarket_prices",
}

@task(show_return_value_in_logs=False,)
def get_api_urls(days_before=1):
    """ Get the urls for daily price of the past 2 days """

    api_urls = {}

    tz = pytz.timezone('Hongkong')

    today = datetime.strftime(datetime.now(tz=tz),'%Y%m%d')
    end_date = datetime.strftime(datetime.now(tz=tz) + timedelta(days = -1),'%Y%m%d')
    start_date = datetime.strftime(datetime.now(tz=tz) + timedelta(days = -1 * days_before),'%Y%m%d')

    horistical_api_list_url = f"https://api.data.gov.hk/v1/historical-archive/list-file-versions?url=https%3A%2F%2Fonline-price-watch.consumer.org.hk%2Fopw%2Fopendata%2Fpricewatch.json&start={start_date}&end={end_date}"
    
    response = requests.get(url=horistical_api_list_url)
    horistical_api_url_ts = json.loads(response.content)["timestamps"]

    api_urls = {ts[:8]: f"https://api.data.gov.hk/v1/historical-archive/get-file?url=https%3A%2F%2Fonline-price-watch.consumer.org.hk%2Fopw%2Fopendata%2Fpricewatch.json&time={ts}" for ts in horistical_api_url_ts}

    api_urls[today] = "https://res.data.gov.hk/api/get-download-file?name=https%3A%2F%2Fonline-price-watch.consumer.org.hk%2Fopw%2Fopendata%2Fpricewatch.json"

    return api_urls

@task(show_return_value_in_logs=False)
def get_price_data(urls: dict):

    daily_data = {}

    for d, url in urls.items():

        response = requests.get(url=url)
        data = json.loads(response.content)
        daily_data[d] = data

    return daily_data


@task
def dump_data_to_bucket(daily_data: dict):

    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    client = Minio("minio:9000", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    for d, data in daily_data.items():

        # Convert JSON object to bytes
        json_data = json.dumps(data).encode("utf-8")

        # Create a BytesIO object
        json_bytes = BytesIO(json_data)

        # Make MINIO_BUCKET_NAME if not exist.
        found = client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            client.make_bucket(MINIO_BUCKET_NAME)

        # Put data in the bucket
        client.put_object(
            MINIO_BUCKET_NAME, f"{d}.json", data=json_bytes, length=len(json_data), content_type="application/json"
        )

@dag(
    schedule="0 3 * * *",
    start_date=datetime(2022, 12, 26),
    catchup=False,
    tags=["prices", "etl"],
    default_args=default_args,
)
def price_etl():

    dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command='cd /usr/app/dbt && dbt deps && dbt build --profiles-dir /usr/app/dbt',
)
    rm_duckdb = BashOperator(
    task_id="rm_duckdb",
    bash_command='rm /data/dbt.duckdb',
)

    dump_data_to_bucket(get_price_data(get_api_urls())) >> rm_duckdb >> dbt_build

price_etl()
