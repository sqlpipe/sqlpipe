package main

import (
	"github.com/calmitchell617/sqlpipe/cmd/initialize"
	"github.com/calmitchell617/sqlpipe/cmd/query"
	"github.com/calmitchell617/sqlpipe/cmd/replication"
	"github.com/calmitchell617/sqlpipe/cmd/serve"
	"github.com/calmitchell617/sqlpipe/cmd/transfer"
	"github.com/calmitchell617/sqlpipe/cmd/version"
	"github.com/calmitchell617/sqlpipe/internal/globals"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sqlpipe",
	Short: "SQLPipe makes it easy to move data between data systems.",
}

var gitHash string
var sqlpipeVersion string

func init() {
	rootCmd.AddCommand(serve.ServeCmd)
	rootCmd.AddCommand(initialize.InitializeCmd)
	rootCmd.AddCommand(transfer.TransferCmd)
	rootCmd.AddCommand(query.QueryCmd)
	rootCmd.AddCommand(replication.ReplicationCmd)

	globals.GitHash = gitHash
	globals.SqlpipeVersion = sqlpipeVersion
	rootCmd.AddCommand(version.VersionCmd)
}

func main() {
	rootCmd.Execute()
}
