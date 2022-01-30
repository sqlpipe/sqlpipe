package version

import (
	"fmt"

	"github.com/calmitchell617/sqlpipe/internal/globals"
	"github.com/spf13/cobra"
)

var (
	VersionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show SQLpipe version",
		Run:   showVersion,
	}
)

func showVersion(cmd *cobra.Command, args []string) {
	fmt.Println(globals.Version)
}
