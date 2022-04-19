package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/globals"
)

var (
	VersionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show SQLpipe version",
		Run:   showVersion,
	}
)

func showVersion(cmd *cobra.Command, args []string) {
	fmt.Println("Git hash:", globals.GitHash)
	fmt.Println("Human version:", globals.SqlpipeVersion)
}
