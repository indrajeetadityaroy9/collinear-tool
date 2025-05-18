#!/usr/bin/env bash
set -euo pipefail

function show_help {
  echo "Usage: ./docker-commands.sh [COMMAND]"
  echo ""
  echo "Commands:"
  echo "  build       - Build all or specific services"
  echo "  up          - Start all services in detached mode"
  echo "  down        - Stop all services"
  echo "  logs        - View logs for all or specific services"
  echo "  shell       - Open a shell in a service container"
  echo "  restart     - Restart all or specific services"
  echo "  clean       - Remove all containers, volumes, and images"
  echo "  status      - Show status of containers"
  echo "  test        - Run tests using pytest"
  echo "  scale       - Scale a service (e.g., './docker-commands.sh scale celery-worker=3')"
  echo "  help        - Show this help message"
  echo ""
  echo "Examples:"
  echo "  ./docker-commands.sh build                 - Build all services"
  echo "  ./docker-commands.sh build api             - Build only the API service"
  echo "  ./docker-commands.sh logs api              - Show logs for the API service"
  echo "  ./docker-commands.sh shell api             - Open a shell in the API container"
  echo "  ./docker-commands.sh scale celery-worker=3 - Scale the celery-worker service to 3 instances"
}

COMMAND=${1:-help}
shift 2>/dev/null || true

case $COMMAND in
  build)
    SERVICE=${1:-}
    if [ -n "$SERVICE" ]; then
      echo "Building service: $SERVICE"
      docker-compose build "$SERVICE"
    else
      echo "Building all services"
      docker-compose build
    fi
    ;;
    
  up)
    echo "Starting all services in detached mode"
    docker-compose up -d
    ;;
    
  down)
    echo "Stopping all services"
    docker-compose down
    ;;
    
  logs)
    SERVICE=${1:-}
    if [ -n "$SERVICE" ]; then
      echo "Showing logs for service: $SERVICE"
      docker-compose logs -f "$SERVICE"
    else
      echo "Showing logs for all services"
      docker-compose logs -f
    fi
    ;;
    
  shell)
    SERVICE=${1:-api}
    echo "Opening shell in service: $SERVICE"
    docker-compose exec "$SERVICE" bash
    ;;
    
  restart)
    SERVICE=${1:-}
    if [ -n "$SERVICE" ]; then
      echo "Restarting service: $SERVICE"
      docker-compose restart "$SERVICE"
    else
      echo "Restarting all services"
      docker-compose restart
    fi
    ;;
    
  clean)
    echo "Removing all containers, volumes, and images"
    docker-compose down --volumes --rmi all
    ;;
    
  status)
    echo "Showing status of services"
    docker-compose ps
    ;;
    
  test)
    echo "Running tests using pytest"
    docker-compose exec api pytest tests/
    ;;
    
  scale)
    if [ -z "${1:-}" ]; then
      echo "Error: Missing scale parameter. Example: './docker-commands.sh scale celery-worker=3'"
      exit 1
    fi
    echo "Scaling service: $1"
    docker-compose up -d --scale "$1"
    ;;
    
  *)
    show_help
    ;;
esac 