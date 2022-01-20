package query

import (
	"fmt"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/engine"
	"github.com/spf13/cobra"
)

var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "Run a query",
	Run:   runQuery,
}

var query data.Query

func init() {
	QueryCmd.Flags().StringVar(&query.Query, "query", "", "Query to run")

	QueryCmd.Flags().StringVar(&query.Connection.DsType, "connection-ds-type", "", "Connection type. Must be one of [postgresql, mysql, mssql, oracle, redshift, snowflake]")
	QueryCmd.Flags().StringVar(&query.Connection.Hostname, "connection-hostname", "", "Connection's hostname")
	QueryCmd.Flags().IntVar(&query.Connection.Port, "connection-port", 0, "Connection's port")
	QueryCmd.Flags().StringVar(&query.Connection.AccountId, "connection-account-id", "", "Connection's account ID (Snowflake only)")
	QueryCmd.Flags().StringVar(&query.Connection.DbName, "connection-db-name", "", "Connection's DB name")
	QueryCmd.Flags().StringVar(&query.Connection.Username, "connection-username", "", "Connection username")
	QueryCmd.Flags().StringVar(&query.Connection.Password, "connection-password", "", "Connection password")
}

func runQuery(cmd *cobra.Command, args []string) {
	err := engine.RunQuery(&query)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Query complete. We make a good team!")
}
