.PHONY: up down logs ps seed test proof-of-flow dbt-run dbt-test generate-events dq-check vector-sync api-search test-e2e clean

# Start all local platform services
up:
	docker compose up -d

# Stop all local platform services
down:
	docker compose down

# Check containers status
ps:
	docker compose ps

# View service logs
logs:
	docker compose logs -f

# Seed PostgreSQL and MongoDB databases with synthetic OTT operational data
seed:
	python scripts/seed_all.py

# Run the proof-of-flow pipeline: Producer -> Kafka -> Consumer -> ClickHouse -> Verification
proof-of-flow:
	python scripts/proof_of_flow_producer.py
	python scripts/proof_of_flow_consumer.py
	python scripts/query_clickhouse.py

# Run synthetic event generator with 1,000 users at 100 EPS
generate-events:
	python apps/event-generator/generate.py --users 1000 --events-per-second 100

# Run dbt transformations across staging, intermediate, and marts
dbt-run:
	cd dbt && dbt run --profiles-dir .

# Run dbt tests across models and data constraints
dbt-test:
	cd dbt && dbt test --profiles-dir .

# Run Data Quality audit suite and quarantine routing
dq-check:
	python data_quality/checks/quality_engine.py

# Compute FastEmbed vectors and sync MongoDB catalog into pgvector
vector-sync:
	python ai/embeddings/embedder.py

# Run full pytest automated test suite
test:
	pytest tests/integration/ tests/e2e/ -v

# Run End-to-End integration tests
test-e2e:
	pytest tests/e2e/ -v

# Run the frontend web application and unified API gateway on http://localhost:8001
frontend:
	uvicorn apps.api.main:app --host 0.0.0.0 --port 8001 --reload

# Teardown and delete persistent volumes
clean:
	docker compose down -v
