up: 
	docker compose build && docker compose up -d

down:
	docker compose down

restart: down up

run-duckdb:
	docker exec -it scheduler /opt/duckdb /app/data/dbt.duckdb -readonly

dbt-doc:
	docker exec -it scheduler bash -c "export DBT_PROFILES_DIR=/usr/app/dbt && cd /usr/app/dbt && dbt docs generate && dbt docs serve"
