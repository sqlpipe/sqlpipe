package data

import (
	"context"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type TransferInfo struct {
	Id             string             `json:"id"`
	CreatedAt      time.Time          `json:"created-at"`
	StoppedAt      string             `json:"stopped-at,omitempty"`
	Status         string             `json:"status"`
	Error          string             `json:"error,omitempty"`
	KeepFiles      bool               `json:"keep-files"`
	TmpDir         string             `json:"tmp-dir"`
	PipeFileDir    string             `json:"pipe-file-dir"`
	FinalCsvDir    string             `json:"final-csv-dir"`
	Context        context.Context    `json:"-"`
	Cancel         context.CancelFunc `json:"-"`
	SourceName     string             `json:"source-instance-name"`
	SourceType     string             `json:"source-type"`
	SourceHostname string             `json:"source-hostname"`
	SourcePort     int                `json:"source-port,omitempty"`
	SourceDatabase string             `json:"source-database"`
	SourceUsername string             `json:"source-username"`
	SourcePassword string             `json:"-"`
	SourceSchema   string             `json:"source-schema,omitempty"`
	SourceTable    string             `json:"source-table,omitempty"`
	// SourceConnectionString        string             `json:"-"`
	TargetName                    string `json:"target-instance-name"`
	TargetType                    string `json:"target-type"`
	TargetConnectionString        string `json:"-"`
	TargetHostname                string `json:"target-hostname"`
	TargetPort                    int    `json:"target-port,omitempty"`
	TargetDatabase                string `json:"target-database"`
	TargetUsername                string `json:"target-username"`
	TargetPassword                string `json:"-"`
	DropTargetTableIfExists       bool   `json:"drop-target-table-if-exists"`
	CreateTargetSchemaIfNotExists bool   `json:"create-target-schema-if-not-exists"`
	CreateTargetTableIfNotExists  bool   `json:"create-target-table-if-not-exists"`
	EntireInstance                bool   `json:"entire-instance"`
	TargetSchema                  string `json:"target-schema,omitempty"`
	TargetTable                   string `json:"target-name"`
	Query                         string `json:"query,omitempty"`
	Delimiter                     string `json:"delimiter"`
	Newline                       string `json:"newline"`
	Null                          string `json:"null"`
	IncrementalColumn             string `json:"incremental-column,omitempty"`
	Vacuum                        bool   `json:"vacuum"`
	PsqlAvailable                 bool   `json:"-"`
	BcpAvailable                  bool   `json:"-"`
	SqlLdrAvailable               bool   `json:"-"`
	TriggeredByCli                bool   `json:"-"`
}

func ValidateTransferInfo(v *validator.Validator, transferInfo *TransferInfo) {

	if !transferInfo.TriggeredByCli {
		validateTransferAutomatedFields(transferInfo)
	}

	if transferInfo.EntireInstance {

		if transferInfo.SourceTable != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or source-table, not both")
		}
		if transferInfo.SourceSchema != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or source-schema, not both")
		}
		if transferInfo.Query != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or query, not both")
		}

		if transferInfo.TargetDatabase != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or target-database, not both")
		}
		if transferInfo.TargetSchema != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or target-schema, not both")
		}
		if transferInfo.TargetTable != "" {
			v.AddFieldError("entire-instance", "you must provide an entire-instance or target-table, not both")
		}

	} else {

		v.NotBlank(transferInfo.TargetTable)

		if transferInfo.Query == "" {
			if transferInfo.SourceTable == "" {
				v.AddFieldError("query", "you must provide a query or source-table to specicy what data to move")
			}
		}

		if transferInfo.Query != "" {
			if transferInfo.SourceTable != "" {
				v.AddFieldError("source-table", "you must provide a query or source-table, not both")
			}

			if transferInfo.SourceSchema != "" {
				v.AddFieldError("source-schema", "you must provide a query or source-schema, not both")
			}
		}
	}

	// v.NotBlank(transferInfo.SourceConnectionString)
	v.NotBlank(transferInfo.TargetConnectionString)

	switch transferInfo.SourceType {
	case "postgresql":
		validatePostgreSQLSource(v, transferInfo)
	case "mysql":
		validateMySQLSource(v, transferInfo)
	case "mssql":
		validateMSSQLSource(v, transferInfo)
	case "oracle":
		validateOracleSource(v, transferInfo)
	case "snowflake":
		validateSnowflakeSource(v, transferInfo)
	default:
		v.AddFieldError("source-type", "source type must be one of postgresql, mysql, mssql, oracle, snowflake")
	}

	switch transferInfo.TargetType {
	case "postgresql":
		validatePostgreSQLTarget(v, transferInfo)
	case "mysql":
		validateMySQLTarget(v, transferInfo)
	case "mssql":
		validateMSSQLTarget(v, transferInfo)
	case "oracle":
		validateOracleTarget(v, transferInfo)
	case "snowflake":
		validateSnowflakeTarget(v, transferInfo)
	default:
		v.AddFieldError("target-type", "target type must be one of postgresql, mysql, mssql, oracle, snowflake")
	}
}

func validatePostgreSQLSource(v *validator.Validator, transferInfo *TransferInfo) {
	if transferInfo.SourceTable != "" {
		v.CheckField(transferInfo.SourceSchema != "", "source-schema", "you must provide a source-schema when providing a source-table for PostgreSQL")
	}
}

func validateMySQLSource(v *validator.Validator, transferInfo *TransferInfo) {
	// v.CheckField(strings.Contains(transferInfo.SourceConnectionString, "parseTime=true"), "source-connection-string", "must contain parseTime=true to move timestamp with time zone data from mysql")
	// v.CheckField(strings.Contains(transferInfo.SourceConnectionString, "loc="), "source-connection-string", `must contain loc=<URL_ENCODED_IANA_TIME_ZONE> ... example: loc=US%2FPacific`)
}

func validateMSSQLSource(v *validator.Validator, transferInfo *TransferInfo) {
	if transferInfo.SourceTable != "" {
		v.CheckField(transferInfo.SourceSchema != "", "source-schema", "you must provide a source-schema when providing a source-table for MSSQL")
	}
}

func validateOracleSource(v *validator.Validator, transferInfo *TransferInfo) {
	if transferInfo.SourceTable != "" {
		v.CheckField(transferInfo.SourceSchema != "", "source-schema", "you must provide a source-schema when providing a source-table for Oracle")
	}
}

func validateSnowflakeSource(v *validator.Validator, transferInfo *TransferInfo) {
	if transferInfo.SourceTable != "" {
		v.CheckField(transferInfo.SourceSchema != "", "source-schema", "you must provide a source-schema when providing a source-table for Snowflake")
	}
}

func validatePostgreSQLTarget(v *validator.Validator, transferInfo *TransferInfo) {
	v.CheckField(transferInfo.PsqlAvailable, "target-type", "you must install psql to transfer data to postgresql")
}

func validateMySQLTarget(v *validator.Validator, transferInfo *TransferInfo) {
	// nothing special needed
}

func validateMSSQLTarget(v *validator.Validator, transferInfo *TransferInfo) {
	v.CheckField(transferInfo.BcpAvailable, "target-type", "you must install bcp to transfer data to mssql")
	v.CheckField(transferInfo.TargetPort != 0, "target-port", "you must provide a target-port for MSSQL")
	v.CheckField(transferInfo.TargetHostname != "", "target-hostname", "you must provide a target-hostname for MSSQL")
	v.CheckField(transferInfo.TargetUsername != "", "target-username", "you must provide a target-username for MSSQL")
	v.CheckField(transferInfo.TargetPassword != "", "target-password", "you must provide a target-password for MSSQL")
	if !transferInfo.EntireInstance {
		v.CheckField(transferInfo.TargetDatabase != "", "target-database", "you must provide a target-database for MSSQL")
	}
}

func validateOracleTarget(v *validator.Validator, transferInfo *TransferInfo) {
	v.CheckField(transferInfo.SqlLdrAvailable, "target-type", "you must install sqlldr to transfer data to oracle")
	v.CheckField(transferInfo.TargetHostname != "", "target-hostname", "you must provide a target-hostname for Oracle")
	v.CheckField(transferInfo.TargetUsername != "", "target-username", "you must provide a target-username for Oracle")
	v.CheckField(transferInfo.TargetPassword != "", "target-password", "you must provide a target-password for Oracle")
	if !transferInfo.EntireInstance {
		v.CheckField(transferInfo.TargetDatabase != "", "target-database", "you must provide a target-database for Oracle")
	}
}

func validateSnowflakeTarget(v *validator.Validator, transferInfo *TransferInfo) {
	// nothing special needed
}

func validateTransferAutomatedFields(transferInfo *TransferInfo) {
	if transferInfo.Id == "" {
		panic("id not set")
	}

	if transferInfo.CreatedAt.IsZero() {
		panic("created-at not set")
	}

	if transferInfo.Context == nil {
		panic("context not set")
	}

	if transferInfo.Cancel == nil {
		panic("cancel not set")
	}
}
