include .envrc

# ==================================================================================== #
# HELPERS
# ==================================================================================== #

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

.PHONY: confirm
confirm:
	@echo -n 'Are you sure? [y/N] ' && read ans && [ $${ans:-N} = y ]

# ==================================================================================== #
# DEVELOPMENT
# ==================================================================================== #

## run/server: build and run a sqlpipe server
.PHONY: run/server
run/server:
	go run ./cmd/sqlpipe server

## db/init: Initialize a fresh instance of postgresql
.PHONY: db/init
db/init:
	docker container rm -f ${DB-CONTAINER-NAME};
	docker container run -d -p 5432:5432 --name ${DB-CONTAINER-NAME} -e POSTGRES_PASSWORD=${POSTGRES-PASSWORD} postgres:14.1

## db/setup: Create a database and user
.PHONY: db/setup
db/setup:
	docker exec -it ${DB-CONTAINER-NAME} psql postgres://postgres:${POSTGRES-PASSWORD}@localhost/postgres?sslmode=disable  -c 'CREATE DATABASE sqlpipe'
	docker exec -it ${DB-CONTAINER-NAME} psql postgres://postgres:${POSTGRES-PASSWORD}@localhost/postgres?sslmode=disable -c "CREATE ROLE sqlpipe WITH LOGIN PASSWORD '${SQLPIPE-PASSWORD}'"
	docker exec -it ${DB-CONTAINER-NAME} psql postgres://postgres:${POSTGRES-PASSWORD}@localhost/sqlpipe?sslmode=disable -c 'CREATE EXTENSION IF NOT EXISTS citext;'

## db/shell: connect to the sqlpipe database as postgres user
.PHONY: db/shell
db/shell:
	docker exec -it ${DB-CONTAINER-NAME} psql postgres://postgres:${POSTGRES-PASSWORD}@localhost/sqlpipe?sslmode=disable

## db/migrations/new name=$1: create a new database migration
.PHONY: db/migrations/new
db/migrations/new:
	@echo 'Creating migration files for ${name}...'
	migrate create -seq -ext=.sql -dir=./migrations ${name}

## db/migrations/up: apply all up database migrations
.PHONY: db/migrations/up
db/migrations/up: confirm
	@echo 'Running up migrations...'
	migrate -path ./migrations -database postgres://postgres:${POSTGRES-PASSWORD}@localhost/sqlpipe?sslmode=disable up

## docker/prune: Prune unused docker stuff
.PHONY: docker/prune
docker/prune:
	@echo 'Pruning unused docker objects'
	docker system prune -f --volumes

# ==================================================================================== #
# QUALITY CONTROL
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

# ==================================================================================== #
# BUILD
# ==================================================================================== #

current_time = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
git_description = $(shell git describe --always --dirty --tags --long)
linker_flags = '-s -X main.buildTime=${current_time} -X main.version=${git_description}'

## build/api: build the cmd/api application
.PHONY: build/api
build/api:
	@echo 'Building cmd/api...'
	go build -ldflags=${linker_flags} -o=./bin/api ./cmd/api
	GOOS=linux GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/linux_arm64/api ./cmd/api
	GOOS=darwin GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/darwin_arm64/api ./cmd/api
	GOOS=windows GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/windows_arm64/api ./cmd/api