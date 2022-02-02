current_time = date -u +"%Y-%m-%dT%H:%M:%SZ"
git_description = git describe --always --dirty --tags --long
linker_flags = '-s -X main.gitHash=${git_description}'

GOOS=linux GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/linux/sqlpipe ./cmd
GOOS=darwin GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/darwin/sqlpipe ./cmd
GOOS=windows GOARCH=arm64 go build -ldflags=${linker_flags} -o=./bin/windows/sqlpipe ./cmd