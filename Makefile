.PHONY: clean-all build up rebuild

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