package main

import (
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/initialize"
	"github.com/calmitchell617/sqlpipe/cmd/sqlpipe/serve"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sqlpipe",
	Short: "SQLPipe makes it easy to move data between data systems.",
}

func init() {
	rootCmd.AddCommand(serve.Serve)
	rootCmd.AddCommand(initialize.Initialize)
}

func main() {
	rootCmd.Execute()
}
