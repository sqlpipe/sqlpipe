package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
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
	logger       *slog.Logger
	version      = vcs.Version()
	globalTmpDir string
	transferInfo data.TransferInfo
)

func main() {

	displayVersion := flag.Bool("version", false, "display version and exit")

	flag.BoolVar(&transferInfo.KeepFiles, "keep-files", false, "keep files after cli transfer")
	flag.StringVar(&transferInfo.SourceName, "source-name", "", "source name")
	flag.StringVar(&transferInfo.SourceType, "source-type", "", "source type")
	flag.StringVar(&transferInfo.SourceConnectionString, "source-connection-string", "", "source connection string")
	flag.StringVar(&transferInfo.TargetName, "target-name", "", "target name")
	flag.StringVar(&transferInfo.TargetType, "target-type", "", "target type")
	flag.StringVar(&transferInfo.TargetConnectionString, "target-connection-string", "", "target connection string")
	flag.StringVar(&transferInfo.TargetHostname, "target-hostname", "", "target hostname")
	flag.IntVar(&transferInfo.TargetPort, "target-port", 0, "target port")
	flag.StringVar(&transferInfo.TargetDatabase, "target-database", "", "target database")
	flag.StringVar(&transferInfo.TargetUsername, "target-username", "", "target username")
	flag.StringVar(&transferInfo.TargetPassword, "target-password", "", "target password")
	flag.BoolVar(&transferInfo.DropTargetTableIfExists, "drop-target-table-if-exists", false, "drop target table if exists")
	flag.BoolVar(&transferInfo.CreateTargetSchemaIfNotExists, "create-target-schema-if-not-exists", false, "create target schema if not exists")
	flag.BoolVar(&transferInfo.CreateTargetTableIfNotExists, "create-target-table-if-not-exists", false, "create target table if not exists")
	flag.StringVar(&transferInfo.SourceSchema, "source-schema", "", "source schema")
	flag.StringVar(&transferInfo.SourceTable, "source-table", "", "source table")
	flag.StringVar(&transferInfo.TargetSchema, "target-schema", "", "target schema")
	flag.StringVar(&transferInfo.TargetTable, "target-table", "", "target table")
	flag.StringVar(&transferInfo.Query, "query", "", "query")
	flag.StringVar(&transferInfo.Delimiter, "delimiter", "{dlm}", "delimiter")
	flag.StringVar(&transferInfo.Newline, "newline", "{nwln}", "newline")
	flag.StringVar(&transferInfo.Null, "null", "{nll}", "null")
	transferInfo.TriggeredByCli = true
	transferInfo.Id = uuid.New().String()

	flag.Parse()

	if *displayVersion {
		fmt.Printf("version:\t%s\n", version)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())

	transferInfo.CreatedAt = time.Now()
	transferInfo.Context = ctx
	transferInfo.Cancel = cancel

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	checkDeps(&transferInfo)

	globalTmpDir = filepath.Join(os.TempDir(), "sqlpipe")
	err := os.MkdirAll(globalTmpDir, 0600)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to create tmp dir :: %v", err))
		os.Exit(1)
	}

	// snowflake driver logs a lot of stuff that we don't want
	sf.GetLogger().SetLogLevel("fatal")

	v := validator.New()

	data.ValidateTransferInfo(v, &transferInfo)

	if !v.Valid() {
		logger.Error(fmt.Sprintf("%+v", v.FieldErrors))
		os.Exit(1)
	}

	err = runTransfer()
	if err != nil {
		logger.Error(fmt.Sprintf("error running transfer :: %v", err))
		os.Exit(1)
	}
}
