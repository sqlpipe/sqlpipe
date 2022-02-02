current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
git_description=$(git describe --always --dirty --tags --long)

GOOS=linux GOARCH=arm64 go build -ldflags='-s -X main.gitHash=${git_description}' -o=./bin/linux/sqlpipe ./cmd
GOOS=darwin GOARCH=arm64 go build -ldflags='-s -X main.gitHash=${git_description}' -o=./bin/darwin/sqlpipe ./cmd
GOOS=windows GOARCH=arm64 go build -ldflags='-s -X main.gitHash=${git_description}' -o=./bin/windows/sqlpipe ./cmd