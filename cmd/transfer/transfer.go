package transfer

import (
	"fmt"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/calmitchell617/sqlpipe/internal/globals"
	"github.com/spf13/cobra"
)

var TransferCmd = &cobra.Command{
	Use:   "transfer",
	Short: "Run a transfer",
	Run:   runTransfer,
}

var transfer data.Transfer

func init() {
	TransferCmd.Flags().StringVar(&transfer.Query, "query", "", "Query to run on source system")
	TransferCmd.Flags().StringVar(&transfer.TargetSchema, "target-schema", "", "Schema to write query results to")
	TransferCmd.Flags().StringVar(&transfer.TargetTable, "target-table", "", "Table to write query results to")
	TransferCmd.Flags().BoolVar(&transfer.Overwrite, "overwrite", false, "Overwrite target table")

	TransferCmd.Flags().StringVar(&transfer.Source.DsType, "source-ds-type", "", "Source type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	TransferCmd.Flags().StringVar(&transfer.Source.Hostname, "source-hostname", "", "Source system's hostname")
	TransferCmd.Flags().IntVar(&transfer.Source.Port, "source-port", 0, "Source system's port")
	TransferCmd.Flags().StringVar(&transfer.Source.AccountId, "source-account-id", "", "Source system's account ID (Snowflake only)")
	TransferCmd.Flags().StringVar(&transfer.Source.DbName, "source-db-name", "", "Source system's DB name")
	TransferCmd.Flags().StringVar(&transfer.Source.Username, "source-username", "", "Source username")
	TransferCmd.Flags().StringVar(&transfer.Source.Password, "source-password", "", "Source password")

	TransferCmd.Flags().StringVar(&transfer.Target.DsType, "target-ds-type", "", "Target type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	TransferCmd.Flags().StringVar(&transfer.Target.Hostname, "target-hostname", "", "Target system's hostname")
	TransferCmd.Flags().IntVar(&transfer.Target.Port, "target-port", 0, "Target system's port")
	TransferCmd.Flags().StringVar(&transfer.Target.AccountId, "target-account-id", "", "Target system's account ID (Snowflake only)")
	TransferCmd.Flags().StringVar(&transfer.Target.DbName, "target-db-name", "", "Target system's DB name")
	TransferCmd.Flags().StringVar(&transfer.Target.Username, "target-username", "", "Target username")
	TransferCmd.Flags().StringVar(&transfer.Target.Password, "target-password", "", "Target password")
	TransferCmd.Flags().BoolVar(&globals.Analytics, "analytics", true, "Send anonymized usage data to SQLpipe for product improvements")
}

func runTransfer(cmd *cobra.Command, args []string) {
	errProperties, err := engine.RunTransfer(&transfer)
	if err != nil {
		fmt.Println(errProperties, err)
		return
	}
	globals.SendAnonymizedTransferAnalytics(transfer, false)
	fmt.Println("Transfer complete. We make a good team!")
}
