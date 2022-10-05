include .envrc

# ==================================================================================== #
# RUN
# ==================================================================================== #

## run/compose: Run the test env using docker compose
.PHONY: run/compose
run/compose: build/docker
	docker-compose down -v \
	&& docker-compose up --build -d \
	&& docker-compose logs -f

## run/sqlpipe: Run SQLpipe locally
.PHONY: run/sqlpipe
run/sqlpipe:
	go run ./cmd/sqlpipe

## restart/compose:
.PHONY: restart/compose
restart/compose: build/docker
	docker rm -f sqlpipe \
	&& docker-compose up --build -d sqlpipe \
	&& docker-compose logs -f

## prod: run the cmd/prod application
.PHONY: run/delve
run/delve: build/delve
	/home/ubuntu/go/bin/dlv exec ./bin/sqlpipe --

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

## build/delve: Build locally with delve friendly flags
.PHONY: build/delve
build/delve:
	go build -o=./bin/sqlpipe ./cmd/sqlpipe

# ==================================================================================== #
# TEST
# ==================================================================================== #

# test/engine: Test the engine
.PHONY: test/engine
test/engine: build/sqlpipe
	go test -v -count=1 -run Connection ./... \
	&& go test -v -count=1 -run Drop ./... \
	&& go test -v -count=1 -run Create ./... \
	&& go test -v -count=1 -run Insert ./... \
	&& go test -v -count=1 -run Transfers ./...

## test/delve: run tests with delve
.PHONY: test/delve
test/delve: build/delve
	docker-compose down -v \
	&& docker-compose up --build -d \
	&& /home/ubuntu/go/bin/dlv test ./internal/engine --

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