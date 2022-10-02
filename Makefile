include .envrc

# ==================================================================================== #
# RUN
# ==================================================================================== #

## run/compose: Run the test env using docker compose
.PHONY: run/compose
run/compose: build/docker
	POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
	docker-compose down -v \
	&& docker-compose up --build -d \
	&& docker-compose logs -f

## run/sqlpipe: Run SQLpipe locally
.PHONY: run/sqlpipe
run/sqlpipe:
	go run ./cmd/sqlpipe

## run/docker:
.PHONY: restart/compose
restart/compose: build/docker
	docker rm -f sqlpipe \
	&& docker-compose up --build -d sqlpipe \
	&& docker-compose logs -f

# ==================================================================================== #
# BUILD
# ==================================================================================== #

## build/sqlpipe: Build SQLpipe locally
.PHONY: build/sqlpipe
build/sqlpipe:
	go build -ldflags="-s" -o=./bin/sqlpipe ./cmd/sqlpipe

## build/docker: Build SQLpipe in Docker
.PHONY: build/docker
build/docker: build/docker
	docker build -t sqlpipe/sqlpipe -f dockerfile .

# ==================================================================================== #
# OTHER
# ==================================================================================== #

## audit: tidy and vendor dependencies and format, vet and test all code
.PHONY: audit
audit: vendor
	@echo 'Formatting code...'
	go fmt ./...
	@echo 'Vetting code...'
	go vet ./...
	staticcheck ./...
	@echo 'Running tests...'
	go test -race -vet=off ./...

## vendor: tidy and vendor dependencies
.PHONY: vendor
vendor:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy
	go mod verify
	@echo 'Vendoring dependencies...'
	go mod vendor