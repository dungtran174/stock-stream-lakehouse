.PHONY: infra-up infra-down superset-init airflow-init airflow-logs clean

# Infrastructure
infra-up:
	docker-compose up -d

infra-down:
	docker-compose down

# Bootstrap Services
superset-init:
	bash superset/bootstrap-superset.sh

airflow-init:
	docker-compose up -d postgres webserver scheduler

airflow-logs:
	docker-compose logs -f webserver scheduler

# Clean up environment
clean:
	docker system prune -f
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
