up: 
	docker compose build && docker compose up -d

down:
	docker compose down

restart: down up

run-duckdb:
	docker exec -it scheduler /opt/duckdb /duckdb/dbt.duckdb -readonly

dbt-doc:
	docker exec -it scheduler bash -c "cd /usr/app/dbt && dbt docs generate && dbt docs serve"
