HUMAN_VERSION="1.0.0"
GIT_HASH=$(git describe --always --dirty --tags --long)

rm -rf ./bin

GOOS=linux GOARCH=amd64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/linux/x86/sqlpipe ./cmd
GOOS=darwin GOARCH=amd64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/macos/x86/sqlpipe ./cmd
GOOS=windows GOARCH=amd64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/windows/x86/sqlpipe.exe ./cmd
GOOS=freebsd GOARCH=amd64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/freebsd/x86/sqlpipe ./cmd
GOOS=linux GOARCH=arm64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/linux/arm/sqlpipe ./cmd
GOOS=darwin GOARCH=arm64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/macos/arm/sqlpipe ./cmd
GOOS=windows GOARCH=arm64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/windows/arm/sqlpipe.exe ./cmd
GOOS=freebsd GOARCH=arm64 go build -ldflags="-s -X main.gitHash=${GIT_HASH} -X main.sqlpipeVersion=${HUMAN_VERSION}" -o=./bin/freebsd/arm/sqlpipe ./cmd

gon ./gon-arm-config.json
gon ./gon-x86-config.json


rm ./bin/macos/arm/sqlpipe
rm ./bin/macos/x86/sqlpipe

mkdir ./bin/${HUMAN_VERSION}
mv ./bin/linux ./bin/${HUMAN_VERSION}
mv ./bin/windows ./bin/${HUMAN_VERSION}
mv ./bin/freebsd ./bin/${HUMAN_VERSION}
mkdir ./bin/${HUMAN_VERSION}/macos
mkdir ./bin/${HUMAN_VERSION}/macos/arm
mkdir ./bin/${HUMAN_VERSION}/macos/x86

unzip ./bin/macos/arm/sqlpipe.zip -d ./bin/${HUMAN_VERSION}/macos/arm
unzip ./bin/macos/x86/sqlpipe.zip -d ./bin/${HUMAN_VERSION}/macos/x86

aws s3 cp ./bin s3://sqlpipe-downloads --recursive --acl public-read