package main

import (
	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "Run a query",
	Run:   runQuery,
}

var query data.Query

func init() {
	QueryCmd.Flags().StringVar(&query.Query, "query", "", "Query to run")

	QueryCmd.Flags().StringVar(&query.Connection.DsType, "connection-ds-type", "", "Connection type")
	QueryCmd.Flags().StringVar(&query.Connection.Hostname, "connection-hostname", "", "Connection's hostname")
	QueryCmd.Flags().IntVar(&query.Connection.Port, "connection-port", 0, "Connection's port")
	QueryCmd.Flags().StringVar(&query.Connection.AccountId, "connection-account-id", "", "Connection's account Id (Snowflake only)")
	QueryCmd.Flags().StringVar(&query.Connection.DbName, "connection-db-name", "", "Connection's DB name")
	QueryCmd.Flags().StringVar(&query.Connection.Username, "connection-username", "", "Connection username")
	QueryCmd.Flags().StringVar(&query.Connection.Password, "connection-password", "", "Connection password")
	QueryCmd.Flags().StringVar(&query.Connection.DriverName, "driver-name", "", "Driver name")
}

func runQuery(cmd *cobra.Command, args []string) {
	RunQuery(query)
}
