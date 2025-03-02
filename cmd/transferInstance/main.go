package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/internal/vcs"

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
	instanceTransfer = &data.InstanceTransfer{
		SourceInstance:   &data.Instance{},
		TransferInfos:    []*data.TransferInfo{},
		NamingConvention: &data.NamingConvention{},
	}
)

func main() {

	flag.StringVar(&instanceTransfer.ID, "instance-transfer-id", "", "The UUID of the instance transfer")
	flag.StringVar(&instanceTransfer.NamingConvention.DatabaseNameInSnowflake, "database-naming-convention", "", "DB naming template")
	flag.StringVar(&instanceTransfer.NamingConvention.SchemaNameInSnowflake, "schema-naming-convention", "", "schema naming template")
	flag.StringVar(&instanceTransfer.NamingConvention.SchemaFallbackInSnowflake, "schema-fallback", "", "schema fallback")
	flag.StringVar(&instanceTransfer.NamingConvention.TableNameInSnowflake, "table-naming-convention", "", "table naming template")
	flag.StringVar(&instanceTransfer.SourceInstance.ID, "source-instance-id", "", "source name")
	flag.StringVar(&instanceTransfer.SourceInstance.CloudProvider, "source-instance-cloud-provider", "", "source cloud provider")
	flag.StringVar(&instanceTransfer.SourceInstance.CloudAccountID, "source-instance-cloud-account-id", "", "source account id")
	flag.StringVar(&instanceTransfer.SourceInstance.Type, "source-instance-type", "", "source type")
	flag.StringVar(&instanceTransfer.SourceInstance.Region, "source-instance-region", "", "source region")
	flag.StringVar(&instanceTransfer.SourceInstance.Host, "source-instance-host", "", "source host")
	flag.IntVar(&instanceTransfer.SourceInstance.Port, "source-instance-port", 0, "source port")
	flag.StringVar(&instanceTransfer.SourceInstance.Username, "source-instance-username", "", "source username")
	flag.StringVar(&instanceTransfer.SourceInstance.Password, "source-instance-password", "", "source password (if blank, a random password will be generated and applied to the instance)")
	flag.StringVar(&instanceTransfer.RestoredInstanceID, "restored-instance-id", "", "backup id")
	flag.StringVar(&instanceTransfer.TargetType, "target-type", "", "target type")
	flag.StringVar(&instanceTransfer.TargetHost, "target-host", "", "target host")
	flag.StringVar(&instanceTransfer.TargetUsername, "target-username", "", "target username")
	flag.StringVar(&instanceTransfer.TargetPassword, "target-password", "", "target password")
	flag.StringVar(&instanceTransfer.CloudUsername, "cloud-username", "", "account username")
	flag.StringVar(&instanceTransfer.CloudPassword, "cloud-password", "", "account password")
	flag.BoolVar(&instanceTransfer.ScanForPII, "scan-for-pii", false, "scan for pii")
	flag.BoolVar(&instanceTransfer.DeleteRestoredInstanceAfterTransfer, "delete-restored-instance-after-transfer", true, "delete restored instance after transfer")
	flag.StringVar(&instanceTransfer.Delimiter, "delimiter", "{dlm}", "delimiter")
	flag.StringVar(&instanceTransfer.Newline, "newline", "{nwln}", "newline")
	flag.StringVar(&instanceTransfer.Null, "null", "{nll}", "null")
	flag.Float64Var(&instanceTransfer.CustomStrategyThreshold, "custom-strategy-threshold", 0.4, "custom strategy threshold")
	flag.Float64Var(&instanceTransfer.CustomStrategyPercentile, "custom-strategy-percentile", 0.5, "custom strategy percentile")
	flag.IntVar(&instanceTransfer.NumRowsToScannForPII, "num-rows-to-scan-for-pii", 1000, "num rows to scan for pii")

	displayVersion := flag.Bool("version", false, "display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("version:\t%s\n", version)
		os.Exit(0)
	}

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	var err error

	if instanceTransfer.DeleteRestoredInstanceAfterTransfer {

		deleteCredentials := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			instanceTransfer.CloudUsername,
			instanceTransfer.CloudPassword,
			"",
		))

		deleteCfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(instanceTransfer.SourceInstance.Region), awsConfig.WithCredentialsProvider(deleteCredentials))
		if err != nil {
			logger.Error("failed to load AWS configuration", "error", err)
			os.Exit(1)
		}

		deleteClient := rds.NewFromConfig(deleteCfg)

		// Input parameters for deletion
		input := &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:   aws.String(instanceTransfer.RestoredInstanceID),
			SkipFinalSnapshot:      aws.Bool(true),
			DeleteAutomatedBackups: aws.Bool(true),
		}

		defer func() {
			_, err = deleteClient.DeleteDBInstance(context.Background(), input)
			if err != nil {
				logger.Error("failed to terminate RDS instance", BackupInstanceId, instanceTransfer.RestoredInstanceID, "error", err)
				os.Exit(1)
			}
		}()
	}

	checkDeps(instanceTransfer)

	globalTmpDir = filepath.Join(os.TempDir(), "sqlpipe")
	err = os.MkdirAll(globalTmpDir, 0600)
	if err != nil {
		logger.Error("failed to create tmp dir", "error", err)
		os.Exit(1)
	}

	// snowflake driver logs a lot of stuff that we don't want
	sf.GetLogger().SetLogLevel("fatal")

	v := validator.New()

	data.ValidateInstanceTransfer(v, instanceTransfer)

	if !v.Valid() {
		logger.Error("invalid instance transfer", "errors", fmt.Sprintf("%+v", v.FieldErrors))
		os.Exit(1)
	}

	err = transferInstance()
	if err != nil {
		logger.Error("error transferring instance", "error", err)
		os.Exit(1)
	}

	logger.Info("instance transfer complete")
}
