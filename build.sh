GOOS=linux GOARCH=arm64 go build -ldflags='-s' -o=./bin/linux/sqlpipe ./cmd
GOOS=darwin GOARCH=arm64 go build -ldflags='-s' -o=./bin/darwin/sqlpipe ./cmd
GOOS=windows GOARCH=arm64 go build -ldflags='-s' -o=./bin/windows/sqlpipe ./cmd