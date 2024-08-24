up: 
	docker compose up --build --detach

down:
	docker compose down

restart: down up

unpause-dag:
	docker exec -it scheduler airflow dags unpause price_etl

run-dag:
	docker exec -it scheduler airflow dags test price_etl

run-duckdb:
	docker exec -it scheduler /opt/duckdb /app/data/dbt.duckdb -readonly

dbt-doc:
	docker exec -it scheduler bash -c "export DBT_PROFILES_DIR=/usr/app/dbt && cd /usr/app/dbt && dbt docs generate && dbt docs serve"
