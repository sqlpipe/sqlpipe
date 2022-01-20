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

## run/serve: build and run a sqlpipe server
.PHONY: run/serve
run/serve:
	go run ./cmd/sqlpipe serve \
		--dsn=postgres://postgres:Mypass123@localhost/sqlpipe?sslmode=disable \
		--secret "8i.(LBH4JZSv#Z@$qKBUcNlUk*C&y}$p"

## run/init: create a new db, set it up, migrate it, then start a new sqlpipe server
.PHONY: run/init
run/init: db/init
	go run ./cmd/sqlpipe serve \
		--admin-username=sqlpipe \
		--admin-password=Mypass123 \
		--dsn=postgres://postgres:Mypass123@localhost/sqlpipe?sslmode=disable \
		--create-admin

## db/init: Initialize a fresh instance of postgresql
.PHONY: db/init
db/init:
	docker container rm -f sqlpipe-postgresql;
	docker container run -d -p 5432:5432 --name sqlpipe-postgresql -e POSTGRES_PASSWORD=Mypass123 postgres:14.1
	sleep 1
	docker exec -it sqlpipe-postgresql psql postgres://postgres:Mypass123@localhost/postgres?sslmode=disable  -c 'CREATE DATABASE sqlpipe'
	go run ./cmd/sqlpipe initialize --dsn=postgres://postgres:Mypass123@localhost/sqlpipe?sslmode=disable --force

## db/shell: connect to the sqlpipe database as postgres user
.PHONY: db/shell
db/shell:
	docker exec -it sqlpipe-postgresql psql postgres://postgres:Mypass123@localhost/sqlpipe?sslmode=disable

## docker/prune: Prune unused docker stuff
.PHONY: docker/prune
docker/prune:
	@echo 'Pruning unused docker objects'
	docker system prune -f --volumes

## env/insert: Insert a few record for testing
.PHONY: env/insert
env/insert:
	@echo 'inserting a few records in each table'
	# insert a non admin user
	curl -u sqlpipe:Mypass123 -k -i -d '{"username": "normalUser", "password": "Mypass123", "admin": false}' https://localhost:9000/api/v1/users
	# insert a connection
	curl -u sqlpipe:Mypass123 -k -i -d '{"name": "prod", "dsType": "postgresql", "hostname": "localhost", "port": 5432, "dbName": "sqlpipe", "username": "postgres", "password": "Mypass123", "skipTest": true}' https://localhost:9000/api/v1/connections
	# insert a transfer
	curl -u sqlpipe:Mypass123 -k -i -d '{"sourceId": 1, "targetId": 1, "query": "select * from connections", "targetSchema": "public", "targetTable": "mytarget", "overwrite": true}' https://localhost:9000/api/v1/transfers
	# insert a couple queries
	curl -u sqlpipe:Mypass123 -k -i -d '{"connectionId": 1, "query": "create table newtable (id int)"}' https://localhost:9000/api/v1/queries
	curl -u sqlpipe:Mypass123 -k -i -d '{"connectionId": 1, "query": "insert into newtable (id) values (1),(2)"}' https://localhost:9000/api/v1/queries

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
	go test -v -race -vet=off ./...

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

## build/sqlpipe: build the cmd/sqlpipe application
.PHONY: build/sqlpipe
build/sqlpipe:
	@echo 'Building cmd/sqlpipe...'
	go build -ldflags=${linker_flags} -o=./bin/sqlpipe ./cmd/sqlpipe
	GOOS=linux GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/linux_arm64/sqlpipe ./cmd/sqlpipe
	GOOS=darwin GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/darwin_arm64/sqlpipe ./cmd/sqlpipe
	GOOS=windows GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/windows_arm64/sqlpipe ./cmd/sqlpipe