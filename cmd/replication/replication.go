package replication

import (
	"fmt"
	"os"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/calmitchell617/sqlpipe/internal/globals"
	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/calmitchell617/sqlpipe/internal/validator"
	"github.com/calmitchell617/sqlpipe/pkg"
	"github.com/spf13/cobra"
)

var replication data.Replication
var force bool

var ReplicationCmd = &cobra.Command{
	Use:   "replicate",
	Short: "Replicate one DB to another via WAL replication",
	Run:   runReplication,
}

func init() {

	ReplicationCmd.Flags().StringVar(&replication.Source.DsType, "source-ds-type", "", "Source type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	ReplicationCmd.Flags().StringVar(&replication.Source.Hostname, "source-hostname", "", "Source system's hostname")
	ReplicationCmd.Flags().IntVar(&replication.Source.Port, "source-port", 0, "Source system's port")
	ReplicationCmd.Flags().StringVar(&replication.Source.AccountId, "source-account-id", "", "Source system's account ID (Snowflake only)")
	ReplicationCmd.Flags().StringVar(&replication.Source.DbName, "source-db-name", "", "Source system's DB name")
	ReplicationCmd.Flags().StringVar(&replication.Source.Username, "source-username", "", "Source username")
	ReplicationCmd.Flags().StringVar(&replication.Source.Password, "source-password", "", "Source password")

	ReplicationCmd.Flags().StringVar(&replication.Target.DsType, "target-ds-type", "", "Target type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	ReplicationCmd.Flags().StringVar(&replication.Target.Hostname, "target-hostname", "", "Target system's hostname")
	ReplicationCmd.Flags().IntVar(&replication.Target.Port, "target-port", 0, "Target system's port")
	ReplicationCmd.Flags().StringVar(&replication.Target.AccountId, "target-account-id", "", "Target system's account ID (Snowflake only)")
	ReplicationCmd.Flags().StringVar(&replication.Target.DbName, "target-db-name", "", "Target system's DB name")
	ReplicationCmd.Flags().StringVar(&replication.Target.Username, "target-username", "", "Target username")
	ReplicationCmd.Flags().StringVar(&replication.Target.Password, "target-password", "", "Target password")
	ReplicationCmd.Flags().StringVar(&replication.TargetSchema, "target-schema", "", "Target schema")
	ReplicationCmd.Flags().BoolVar(&globals.Analytics, "analytics", true, "Send anonymized usage data to SQLpipe for product improvements")

	ReplicationCmd.Flags().StringSliceVar(&replication.Tables, "tables", []string{}, "Specify the tables you want replicated, with a schema name if necessary.")
	ReplicationCmd.Flags().StringVar(&replication.ReplicationSlot, "replication-slot", "sqlpipe_slot", "Replication slot name")

	ReplicationCmd.Flags().BoolVar(&force, "force", false, "Do not ask for confirmation")

}

func runReplication(cmd *cobra.Command, args []string) {
	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)
	v := validator.New()
	if validateReplication(v); !v.Valid() {
		logger.PrintInfo("Exiting.", v.Errors)
		return
	}

	if !force {
		confirmed := pkg.Confirm(fmt.Sprintf(`replicate the db? This will drop the publication "%v", if it exists, and recreate it. it will create a replication slot by the same name and subscribe to it.`, replication.ReplicationSlot))
		if !confirmed {
			logger.PrintInfo("Exiting.", nil)
			return
		}
	}

	errProperties, err := engine.RunReplication(&replication)
	if err != nil {
		fmt.Println(errProperties, err)
		return
	}
}

func validateReplication(v *validator.Validator) {
	v.Check(replication.Source.DsType == "postgresql", "source-ds-type", "The only source system that this version of SQLpipe can replicate from is PostgreSQL at this time.")
	v.Check(replication.Source.Hostname != "", "source-hostname", "A source hostname is required")
	v.Check(replication.Source.Port != 0, "source-port", "A source port is required")
	v.Check(replication.Source.DbName != "", "source-db-name", "A source DB name is required")
	v.Check(replication.Source.Username != "", "source-username", "A source username is required")
	v.Check(replication.Source.Password != "", "source-password", "A source password is required")

	switch replication.Target.DsType {
	case "snowflake":
		v.Check(replication.Target.AccountId != "", "target-account-id", "A target account ID is required for Snowflake connections")
		v.Check(replication.TargetSchema != "", "target-schema", "A target schema is required for Snowflake connections")
	case "postgresql":
		v.Check(replication.TargetSchema != "", "target-schema", "A target schema is required for postgresql connections")
		v.Check(replication.Target.Hostname != "", "target-hostname", "A target hostname is required")
		v.Check(replication.Target.Port != 0, "target-port", "A target port is required")
	case "mysql":
		v.Check(replication.TargetSchema == "", "target-schema", "target schema must be nil for mysql connections")
		v.Check(replication.Target.Hostname != "", "target-hostname", "A target hostname is required")
		v.Check(replication.Target.Port != 0, "target-port", "A target port is required")
	case "mssql":
		v.Check(replication.TargetSchema != "", "target-schema", "A target schema is required for mssql connections")
		v.Check(replication.Target.Hostname != "", "target-hostname", "A target hostname is required")
		v.Check(replication.Target.Port != 0, "target-port", "A target port is required")
	case "oracle":
		v.Check(replication.TargetSchema == "", "target-schema", "target schema must be nil for oracle connections")
		v.Check(replication.Target.Hostname != "", "target-hostname", "A target hostname is required")
		v.Check(replication.Target.Port != 0, "target-port", "A target port is required")
	case "redshift":
		v.Check(replication.TargetSchema != "", "target-schema", "A target schema is required for redshift connections")
		v.Check(replication.Target.Hostname != "", "target-hostname", "A target hostname is required")
		v.Check(replication.Target.Port != 0, "target-port", "A target port is required")
	}
	v.Check(replication.Target.DbName != "", "target-db-name", "A target DB name is required")
	v.Check(replication.Target.Username != "", "target-username", "A target username is required")
	v.Check(replication.Target.Password != "", "target-password", "A target password is required")

	v.Check(replication.ReplicationSlot != "", "replication-slot", "A replication slot name is required")
	v.Check(len(replication.Tables) > 0, "tables", "You must specify at least one table to replicate")
}
