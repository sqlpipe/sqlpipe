include .envrc

## run/sqlpipe: run the cmd/sqlpipe application
.PHONY: run/sqlpipe
run/sqlpipe:
	go run ./cmd/sqlpipe -secure=false

## run/docker: run sqlpipe testing stack in docker
.PHONY: run/docker
run/docker: 
	POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
		docker-compose down -v \
		&& docker-compose up --build -d \
		&& docker-compose logs -f

## restart/docker: restart sqlpipe in docker
.PHONY: restart/docker
restart/docker: 
		docker-compose up --build -d sqlpipe \
		&& docker-compose logs -f

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

## build/sqlpipe: build the cmd/sqlpipe application
.PHONY: build/sqlpipe
build/sqlpipe:
	@echo 'Building cmd/sqlpipe...'
	go build -ldflags="-s" -o=./bin/sqlpipe ./cmd/sqlpipe
	GOOS=linux GOARCH=arm64 go build -ldflags="-s" -o=./bin/linux_arm64/sqlpipe ./cmd/sqlpipe