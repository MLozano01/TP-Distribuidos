default: build

# ej de uso: make docker-image-filter
docker-image-%:
	@echo "Building Docker image : $*"
	docker build -f "./$*/Dockerfile" -t "$*:latest" .

.PHONY: docker-image-%

docker-run:
	docker compose -f docker-compose.yaml up
.PHONY: docker-run

all:

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./data_controller/Dockerfile -t "data-controller:latest" .
	docker build -f ./filter/Dockerfile -t "filter:latest" .
	docker build -f ./transformer/Dockerfile -t "transformer:latest" .
	docker build -f ./aggregator/Dockerfile -t "aggregator:latest" .
	docker build -f ./reducer/Dockerfile -t "reducer:latest" .
	docker build -f ./joiner/Dockerfile -t "joiner:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose --profile "*" -f docker-compose.yaml up --build
.PHONY: docker-compose-up

docker-compose-up-system: docker-image
	docker compose -f docker-compose.yaml up --build -d
.PHONY: docker-compose-up-system

docker-compose-up-clients: docker-image-client
	docker compose --profile clients -f docker-compose.yaml up -d
.PHONY: docker-compose-up-clients

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
	docker network prune -f
	docker volume prune -f
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

