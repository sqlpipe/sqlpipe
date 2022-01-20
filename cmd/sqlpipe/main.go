package main

import (
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/initialize"
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/query"
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/serve"
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/transfer"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sqlpipe",
	Short: "SQLPipe makes it easy to move data between data systems.",
}

func init() {
	rootCmd.AddCommand(serve.ServeCmd)
	rootCmd.AddCommand(initialize.InitializeCmd)
	rootCmd.AddCommand(transfer.TransferCmd)
	rootCmd.AddCommand(query.QueryCmd)
}

func main() {
	rootCmd.Execute()
}
