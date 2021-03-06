package query

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine"
	"github.com/sqlpipe/sqlpipe/internal/globals"
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
	errProperties, err := engine.RunQuery(&query)
	if err != nil {
		fmt.Println(errProperties, err)
		return
	}
	globals.SendAnonymizedQueryAnalytics(query, false)
	fmt.Println("Query complete. We make a good team!")
}
