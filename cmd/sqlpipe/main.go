package main

import (
	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/globals"
)

var rootCmd = &cobra.Command{
	Use:   "sqlpipe",
	Short: "SQLPipe makes it easy to move data between data systems.",
}

var gitHash string
var sqlpipeVersion string

func init() {
	// rootCmd.AddCommand(serve.ServeCmd)
	// rootCmd.AddCommand(initialize.InitializeCmd)
	// rootCmd.AddCommand(transfer.TransferCmd)
	rootCmd.AddCommand(QueryCmd)

	globals.GitHash = gitHash
	globals.SqlpipeVersion = sqlpipeVersion
	rootCmd.AddCommand(VersionCmd)
}

func main() {
	rootCmd.Execute()
}
