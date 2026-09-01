.PHONY: infra-up infra-down clean

# Infrastructure
infra-up:
	docker-compose up -d

infra-down:
	docker-compose down

# Clean up environment
clean:
	docker system prune -f
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
