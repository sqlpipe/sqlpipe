package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/internal/vcs"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	sf "github.com/snowflakedb/gosnowflake"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

var (
	logger           *slog.Logger
	version          = vcs.Version()
	globalTmpDir     string
	instanceTransfer data.InstanceTransfer
)

func main() {

	flag.StringVar(&instanceTransfer.Id, "id", "", "id")
	flag.StringVar(&instanceTransfer.SourceName, "source-name", "", "source name")
	flag.StringVar(&instanceTransfer.SourceType, "source-type", "", "source type")
	flag.StringVar(&instanceTransfer.SourceHostname, "source-hostname", "", "source hostname")
	flag.IntVar(&instanceTransfer.SourcePort, "source-port", 0, "source port")
	flag.StringVar(&instanceTransfer.SourceUsername, "source-username", "", "source username")
	flag.StringVar(&instanceTransfer.SourcePassword, "source-password", "", "source password")
	flag.StringVar(&instanceTransfer.TargetName, "target-name", "", "target name")
	flag.StringVar(&instanceTransfer.TargetType, "target-type", "", "target type")
	flag.StringVar(&instanceTransfer.TargetHostname, "target-hostname", "", "target hostname")
	flag.IntVar(&instanceTransfer.TargetPort, "target-port", 0, "target port")
	flag.StringVar(&instanceTransfer.TargetUsername, "target-username", "", "target username")
	flag.StringVar(&instanceTransfer.TargetPassword, "target-password", "", "target password")
	flag.StringVar(&instanceTransfer.Delimiter, "delimiter", "{dlm}", "delimiter")
	flag.StringVar(&instanceTransfer.Newline, "newline", "{nwln}", "newline")
	flag.StringVar(&instanceTransfer.Null, "null", "{nll}", "null")
	flag.StringVar(&instanceTransfer.AccountID, "account-id", "", "account id")
	flag.StringVar(&instanceTransfer.Region, "region", "", "region")
	flag.StringVar(&instanceTransfer.AccountUsername, "account-username", "", "account username")
	flag.StringVar(&instanceTransfer.AccountPassword, "account-password", "", "account password")
	flag.StringVar(&instanceTransfer.BackupId, "backup-id", "", "backup id")

	displayVersion := flag.Bool("version", false, "display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("version:\t%s\n", version)
		os.Exit(0)
	}

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	deleteCredentials := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
		instanceTransfer.AccountUsername,
		instanceTransfer.AccountPassword,
		"",
	))

	deleteCfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(instanceTransfer.Region), awsConfig.WithCredentialsProvider(deleteCredentials))
	if err != nil {
		logger.Error(fmt.Sprintf("failed to load AWS configuration :: %v", err))
		os.Exit(1)
	}

	// Create an RDS client
	deleteClient := rds.NewFromConfig(deleteCfg)

	// Input parameters for deletion
	input := &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String(instanceTransfer.BackupId),
		SkipFinalSnapshot:      aws.Bool(true),
		DeleteAutomatedBackups: aws.Bool(true),
	}

	defer func() {
		_, err = deleteClient.DeleteDBInstance(context.TODO(), input)
		if err != nil {
			logger.Error(fmt.Sprintf("failed to delete RDS instance %v :: %v", instanceTransfer.BackupId, err))
			os.Exit(1)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	instanceTransfer.CreatedAt = time.Now()
	instanceTransfer.Context = ctx
	instanceTransfer.Cancel = cancel

	checkDeps(&instanceTransfer)

	globalTmpDir = filepath.Join(os.TempDir(), "sqlpipe")
	err = os.MkdirAll(globalTmpDir, 0600)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to create tmp dir :: %v", err))
		os.Exit(1)
	}

	// snowflake driver logs a lot of stuff that we don't want
	sf.GetLogger().SetLogLevel("fatal")

	v := validator.New()

	data.ValidateInstanceTransfer(v, &instanceTransfer)

	if !v.Valid() {
		logger.Error(fmt.Sprintf("%+v", v.FieldErrors))
		os.Exit(1)
	}

	err = transferInstance()
	if err != nil {
		logger.Error(fmt.Sprintf("error transferring instance :: %v", err))
	}
}
