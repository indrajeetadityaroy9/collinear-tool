.PHONY: clean-all build up rebuild cache-status


clean-all:
	docker-compose down --volumes --remove-orphans
	docker system prune -af
	docker volume prune -f
	-docker rmi $$(docker images -aq)


build:
	docker-compose build --no-cache


up:
	docker-compose up


rebuild: clean-all build up


cache-status:
	docker compose exec api python -c "import redis; r = redis.Redis(host='redis', port=6379, decode_responses=True); print('Datasets cached:', r.zcard('hf:datasets:all:zset'))"