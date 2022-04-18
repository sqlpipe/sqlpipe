package main

import (
	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/cmd/query"
	"github.com/sqlpipe/sqlpipe/cmd/version"
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
	rootCmd.AddCommand(query.QueryCmd)

	globals.GitHash = gitHash
	globals.SqlpipeVersion = sqlpipeVersion
	rootCmd.AddCommand(version.VersionCmd)
}

func main() {
	rootCmd.Execute()
}
