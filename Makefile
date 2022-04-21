include .envrc

## run: build and run sqlpipe docker image in background
.PHONY: run
run:
	docker build -t sqlpipe:2.0.0 .
	docker container rm -f sqlpipe
	docker run -d --name sqlpipe -p 9000:9000 sqlpipe:2.0.0

## shell: build docker image then get a shell inside
.PHONY: shell
shell: run
	docker exec -it sqlpipe bash

## serve: build and run a sqlpipe server in docker
.PHONY: serve
serve: run
	docker exec -it sqlpipe sqlpipe serve \
    	--etcd-cluster \
    	--etcd-endpoints "http://172.31.13.46:2379" \
		--etcd-password ${ETCD_PASSWORD}

## init: build sqlpipe and initialize etcd through it
.PHONY: init
init: run
	docker exec -it sqlpipe sqlpipe initialize \
    --etcd-endpoints "http://172.31.13.46:2379" \
    --etcd-root-password ${ETCD_PASSWORD} \
    --etcd-sqlpipe-password ${ETCD_PASSWORD} \
	--sqlpipe-admin-password ${SQLPIPE_ADMIN_PASSWORD}

# spinup: Spinup cloud db instances
.PHONY: spinup
spinup:
	# aws rds create-db-instance \
	# 	--db-instance-identifier sqlpipe-test-postgresql \
	# 	--db-name testing \
	# 	--backup-retention-period 0 \
	# 	--db-instance-class db.t3.micro \
	# 	--engine postgres \
	# 	--no-multi-az \
	# 	--vpc-security-group-ids ${rdsSecurityGroup} \
	# 	--master-username sqlpipe \
	# 	--master-user-password ${SQLPIPE-PASSWORD} \
	# 	--storage-type gp2 \
	# 	--allocated-storage 20 \
	# 	--no-enable-performance-insights >/dev/null;

	# aws rds create-db-instance \
	# 	--db-instance-identifier sqlpipe-test-mysql \
	# 	--db-name testing \
	# 	--backup-retention-period 0 \
	# 	--db-instance-class db.t3.micro \
	# 	--engine mysql \
	# 	--no-multi-az \
	# 	--vpc-security-group-ids ${rdsSecurityGroup} \
	# 	--master-username sqlpipe \
	# 	--master-user-password ${SQLPIPE-PASSWORD} \
	# 	--storage-type gp2 \
	# 	--allocated-storage 20 \
	# 	--no-enable-performance-insights >/dev/null;

	aws rds create-db-instance \
		--db-instance-identifier sqlpipe-test-mssql \
		--backup-retention-period 0 \
		--db-instance-class db.t3.small \
		--engine sqlserver-web \
		--no-multi-az \
		--vpc-security-group-ids ${rdsSecurityGroup} \
		--master-username sqlpipe \
		--master-user-password ${SQLPIPE-PASSWORD} \
		--storage-type gp2 \
		--allocated-storage 20 \
		--license-model license-included \
		--no-enable-performance-insights >/dev/null;

	# aws rds create-db-instance \
	# 	--db-instance-identifier sqlpipe-test-oracle \
	# 	--db-name testing \
	# 	--backup-retention-period 0 \
	# 	--db-instance-class db.t3.small \
	# 	--engine oracle-se2 \
	# 	--no-multi-az \
	# 	--vpc-security-group-ids ${rdsSecurityGroup} \
	# 	--master-username sqlpipe \
	# 	--master-user-password ${SQLPIPE-PASSWORD} \
	# 	--storage-type gp2 \
	# 	--allocated-storage 20 \
	# 	--license-model license-included \
	# 	--no-enable-performance-insights >/dev/null;

	# aws redshift create-cluster \
	# 	--node-type dc2.large \
	# 	--master-username sqlpipe \
	# 	--db-name testing \
	# 	--cluster-type single-node \
	# 	--master-user-password ${SQLPIPE-PASSWORD} \
	# 	--vpc-security-group-ids ${rdsSecurityGroup} \
	# 	--cluster-identifier sqlpipe-test-redshift >/dev/null;

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
linker_flags = '-s -X main.gitHash=${git_description}'

## build: build the application locally
.PHONY: build
build:
	@echo 'Building sqlpipe'
	go build -ldflags=${linker_flags} -o=./bin/sqlpipe ./cmd

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