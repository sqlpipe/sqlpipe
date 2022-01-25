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

## db/backend: connect to the backend database as postgres user
.PHONY: db/backend
db/backend:
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

# env/spinup: Spinup cloud instances
.PHONY: env/spinup
env/spinup:
	aws rds create-db-instance \
		--db-instance-identifier sqlpipe-test-postgresql \
		--db-name testing \
		--backup-retention-period 0 \
		--db-instance-class db.t3.micro \
		--engine postgres \
		--no-multi-az \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--master-username sqlpipe \
		--master-user-password Mypass123 \
		--storage-type gp2 \
		--allocated-storage 20 \
		--no-enable-performance-insights >/dev/null;

	aws rds create-db-instance \
		--db-instance-identifier sqlpipe-test-mysql \
		--db-name testing \
		--backup-retention-period 0 \
		--db-instance-class db.t3.micro \
		--engine mysql \
		--no-multi-az \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--master-username sqlpipe \
		--master-user-password Mypass123 \
		--storage-type gp2 \
		--allocated-storage 20 \
		--no-enable-performance-insights >/dev/null;

	aws rds create-db-instance \
		--db-instance-identifier sqlpipe-test-mssql \
		--backup-retention-period 0 \
		--db-instance-class db.t3.small \
		--engine sqlserver-web \
		--no-multi-az \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--master-username sqlpipe \
		--master-user-password Mypass123 \
		--storage-type gp2 \
		--allocated-storage 20 \
		--license-model license-included \
		--no-enable-performance-insights >/dev/null;

	aws rds create-db-instance \
		--db-instance-identifier sqlpipe-test-oracle \
		--db-name testing \
		--backup-retention-period 0 \
		--db-instance-class db.t3.small \
		--engine oracle-se2 \
		--no-multi-az \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--master-username sqlpipe \
		--master-user-password Mypass123 \
		--storage-type gp2 \
		--allocated-storage 20 \
		--license-model license-included \
		--no-enable-performance-insights >/dev/null;

	aws redshift create-cluster \
		--node-type dc2.large \
		--master-username sqlpipe \
		--db-name testing \
		--cluster-type single-node \
		--master-user-password Mypass123 \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--cluster-identifier sqlpipe-test-redshift >/dev/null;

# env/teardown: Spin down cloud instances
.PHONY: env/teardown
env/teardown:
	# aws rds delete-db-instance --db-instance-identifier sqlpipe-test-postgresql --skip-final-snapshot &> /dev/null;
	# aws rds delete-db-instance --db-instance-identifier sqlpipe-test-mysql --skip-final-snapshot &> /dev/null;
	# aws rds delete-db-instance --db-instance-identifier sqlpipe-test-mssql --skip-final-snapshot &> /dev/null;
	# aws rds delete-db-instance --db-instance-identifier sqlpipe-test-oracle --skip-final-snapshot &> /dev/null;
	aws redshift delete-cluster --cluster-identifier sqlpipe-test-redshift --skip-final-cluster-snapshot &> /dev/null;

# db/postgresql: Open shell to PostgreSQL testing DB
.PHONY: db/postgresql
db/postgresql:
	PGPASSWORD=${postgresqlPassword} psql -h ${postgresqlHostname} -U ${postgresqlUsername} -d ${postgresqlDbName}

# db/redshift: Open shell to redshift testing DB
.PHONY: db/redshift
db/redshift:
	PGPASSWORD=${redshiftPassword} psql -h ${redshiftHostname} -U ${redshiftUsername} -d ${redshiftDbName} -p 5439

# db/mysql: Open shell to MySQL testing DB
.PHONY: db/mysql
db/mysql:
	mysql -h ${mysqlHostname} -u ${mysqlUsername} --password=${mysqlPassword} -D ${mysqlDbName}

# db/mssql: Open shell to MSSQL testing DB
.PHONY: db/mssql
db/mssql:
	sqlcmd -S ${mssqlHostname}

# test: Test stuff
.PHONY: test
test:
	go test -v -count=1 ./...

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
	# staticcheck ./...
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