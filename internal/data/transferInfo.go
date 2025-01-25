package data

import (
	"context"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type TransferInfo struct {
	ID                            string   `json:"id"`
	Error                         string   `json:"error,omitempty"`
	KeepFiles                     bool     `json:"keep-files"`
	TmpDir                        string   `json:"tmp-dir"`
	PipeFileDir                   string   `json:"pipe-file-dir"`
	FinalCsvDir                   string   `json:"final-csv-dir"`
	SourceInstance                Instance `json:"source-instance"`
	SourceDatabase                string   `json:"source-database"`
	SourceSchema                  string   `json:"source-schema,omitempty"`
	SourceTable                   string   `json:"source-table,omitempty"`
	TargetType                    string   `json:"target-type"`
	TargetConnectionString        string   `json:"-"`
	TargetHost                    string   `json:"target-host"`
	TargetPort                    int      `json:"target-port,omitempty"`
	TargetDatabase                string   `json:"target-database"`
	TargetUsername                string   `json:"target-username"`
	TargetPassword                string   `json:"-"`
	DropTargetTableIfExists       bool     `json:"drop-target-table-if-exists"`
	CreateTargetSchemaIfNotExists bool     `json:"create-target-schema-if-not-exists"`
	CreateTargetTableIfNotExists  bool     `json:"create-target-table-if-not-exists"`
	TargetSchema                  string   `json:"target-schema,omitempty"`
	TargetTable                   string   `json:"target-name"`
	Query                         string   `json:"query,omitempty"`
	Delimiter                     string   `json:"delimiter"`
	Newline                       string   `json:"newline"`
	Null                          string   `json:"null"`
	PsqlAvailable                 bool     `json:"-"`
	BcpAvailable                  bool     `json:"-"`
	SqlLdrAvailable               bool     `json:"-"`
	StagingDbName                 string   `json:"staging-db-name"`
	TableNode                     *SafeTreeNode
	ColumnInfos                   []*ColumnInfo
	Context                       context.Context
	Cancel                        context.CancelFunc
}

type ColumnInfo struct {
	Name         string `json:"name"`
	PipeType     string `json:"pipe-type"`
	ScanType     string `json:"scan-type"`
	DecimalOk    bool   `json:"decimal-ok"`
	Precision    int64  `json:"precision"`
	Scale        int64  `json:"scale"`
	LengthOk     bool   `json:"length-ok"`
	Length       int64  `json:"length"`
	NullableOk   bool   `json:"nullable-ok"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is-primary-key"`
}

func ValidateTransferInfo(v *validator.Validator, transferInfo *TransferInfo) {

	validateTransferAutomatedFields(transferInfo)

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

	// v.NotBlank(transferInfo.SourceConnectionString)
	v.NotBlank(transferInfo.TargetConnectionString)

	switch transferInfo.SourceInstance.Type {
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
	v.CheckField(transferInfo.TargetHost != "", "target-host", "you must provide a target host for MSSQL")
	v.CheckField(transferInfo.TargetUsername != "", "target-username", "you must provide a target-username for MSSQL")
	v.CheckField(transferInfo.TargetPassword != "", "target-password", "you must provide a target-password for MSSQL")
	v.CheckField(transferInfo.TargetDatabase != "", "target-database", "you must provide a target-database for MSSQL")
}

func validateOracleTarget(v *validator.Validator, transferInfo *TransferInfo) {
	v.CheckField(transferInfo.SqlLdrAvailable, "target-type", "you must install sqlldr to transfer data to oracle")
	v.CheckField(transferInfo.TargetHost != "", "target-host", "you must provide a target host for Oracle")
	v.CheckField(transferInfo.TargetUsername != "", "target-username", "you must provide a target-username for Oracle")
	v.CheckField(transferInfo.TargetPassword != "", "target-password", "you must provide a target-password for Oracle")
	v.CheckField(transferInfo.TargetDatabase != "", "target-database", "you must provide a target-database for Oracle")
}

func validateSnowflakeTarget(v *validator.Validator, transferInfo *TransferInfo) {
	// nothing special needed
}

func validateTransferAutomatedFields(transferInfo *TransferInfo) {
	if transferInfo.ID == "" {
		panic("id not set")
	}

	if transferInfo.Context == nil {
		panic("context not set")
	}

	if transferInfo.Cancel == nil {
		panic("cancel not set")
	}
}
