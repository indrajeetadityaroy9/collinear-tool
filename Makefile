.PHONY: clean-all build up rebuild cache-status

# Remove all stopped containers, images, and volumes (DANGEROUS: use with care)
clean-all:
	docker-compose down --volumes --remove-orphans
	docker system prune -af
	docker volume prune -f
	-docker rmi $$(docker images -aq)

# Build all images from scratch
build:
	docker-compose build --no-cache

# Start the stack
up:
	docker-compose up

# Clean, build, and start everything fresh
rebuild: clean-all build up

# Check the number of cached datasets in Redis
cache-status:
	docker compose exec api python -c "import redis; r = redis.Redis(host='redis', port=6379, decode_responses=True); print('Datasets cached:', r.zcard('hf:datasets:all:zset'))" 