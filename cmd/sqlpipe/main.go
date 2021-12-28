package main

import (
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/server"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sqlpipe",
	Short: "SQLPipe makes it easy to move data between data systems.",
}

func init() {
	rootCmd.AddCommand(server.Serve)
	// rootCmd.AddCommand(transfer.TransferCmd)
	// rootCmd.AddCommand(query.QueryCmd)
}

func main() {
	rootCmd.Execute()
}
