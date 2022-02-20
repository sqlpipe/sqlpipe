package sync

import (
	"fmt"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/calmitchell617/sqlpipe/internal/globals"
	"github.com/calmitchell617/sqlpipe/internal/validator"
	"github.com/k0kubun/pp/v3"
	"github.com/spf13/cobra"
)

var sync data.Sync

var SyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync one DB to another via WAL replication",
	Run:   runSync,
}

func init() {

	SyncCmd.Flags().StringVar(&sync.Source.DsType, "source-ds-type", "", "Source type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	SyncCmd.Flags().StringVar(&sync.Source.Hostname, "source-hostname", "", "Source system's hostname")
	SyncCmd.Flags().IntVar(&sync.Source.Port, "source-port", 0, "Source system's port")
	SyncCmd.Flags().StringVar(&sync.Source.AccountId, "source-account-id", "", "Source system's account ID (Snowflake only)")
	SyncCmd.Flags().StringVar(&sync.Source.DbName, "source-db-name", "", "Source system's DB name")
	SyncCmd.Flags().StringVar(&sync.Source.Username, "source-username", "", "Source username")
	SyncCmd.Flags().StringVar(&sync.Source.Password, "source-password", "", "Source password")

	SyncCmd.Flags().StringVar(&sync.Target.DsType, "target-ds-type", "", "Target type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	SyncCmd.Flags().StringVar(&sync.Target.Hostname, "target-hostname", "", "Target system's hostname")
	SyncCmd.Flags().IntVar(&sync.Target.Port, "target-port", 0, "Target system's port")
	SyncCmd.Flags().StringVar(&sync.Target.AccountId, "target-account-id", "", "Target system's account ID (Snowflake only)")
	SyncCmd.Flags().StringVar(&sync.Target.DbName, "target-db-name", "", "Target system's DB name")
	SyncCmd.Flags().StringVar(&sync.Target.Username, "target-username", "", "Target username")
	SyncCmd.Flags().StringVar(&sync.Target.Password, "target-password", "", "Target password")
	SyncCmd.Flags().BoolVar(&globals.Analytics, "analytics", true, "Send anonymized usage data to SQLpipe for product improvements")

	SyncCmd.Flags().StringSliceVar(&sync.Tables, "tables", []string{}, "Specify the tables you want synced, with a schema name if necessary.")
	SyncCmd.Flags().StringVar(&sync.ReplicationSlot, "replication-slot", "", "Replication slot name")

}

func runSync(cmd *cobra.Command, args []string) {
	v := validator.New()
	if validateSync(v); !v.Valid() {
		fmt.Printf("\nErrors validating your input:\n\n")
		pp.Print(v.Errors)
		return
	}
	errProperties, err := engine.RunSync(&sync)
	if err != nil {
		fmt.Println(errProperties, err)
		return
	}
}

func validateSync(v *validator.Validator) {
	v.Check(sync.Source.DsType == "postgresql", "source-ds-type", "The only source system that this version of SQLpipe can sync from is PostgreSQL at this time.")
	v.Check(sync.Source.Hostname != "", "source-hostname", "A source hostname is required")
	v.Check(sync.Source.Port != 0, "source-port", "A source port is required")
	v.Check(sync.Source.DbName != "", "source-db-name", "A source DB name is required")
	v.Check(sync.Source.Username != "", "source-username", "A source username is required")
	v.Check(sync.Source.Password != "", "source-password", "A source password is required")

	v.Check(sync.Target.DsType == "snowflake", "target-ds-type", "The only target system that this version of SQLpipe can sync to is Snowflake at this time.")
	v.Check(sync.Target.AccountId != "", "target-account-id", "A target account ID is required")
	v.Check(sync.Target.DbName != "", "target-db-name", "A target DB name is required")
	v.Check(sync.Target.Username != "", "target-username", "A target username is required")
	v.Check(sync.Target.Password != "", "target-password", "A target password is required")

	v.Check(sync.ReplicationSlot != "", "replication-slot", "A replication slot name is required")
	v.Check(len(sync.Tables) > 0, "tables", "You must specify at least one table to sync")
}
