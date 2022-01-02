package initialize

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/spf13/cobra"
)

var (
	Initialize = &cobra.Command{
		Use:   "initialize",
		Short: "Initialize PostgreSQL DB.",
		Run:   initialize,
	}

	dsn   string
	force bool

	createUsers = `
		CREATE TABLE users (
			id bigserial PRIMARY KEY,
			created_at timestamp(0) with time zone NOT NULL DEFAULT NOW(),
			username text UNIQUE NOT NULL,
			password_hash bytea NOT NULL,
			admin bool NOT NULL DEFAULT false,
			version integer NOT NULL DEFAULT 1
		);
	`
)

func init() {
	Initialize.Flags().StringVar(&dsn, "dsn", "", "Database backend connection string")
	Initialize.Flags().BoolVar(&force, "force", false, "Do not ask for confirmation")
}

func initialize(cmd *cobra.Command, args []string) {
	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)

	if dsn == "" {
		logger.PrintFatal(errors.New("you must supply a database connection string, or DSN, to initialize a DB"), nil)
	}

	if !force {
		confirmed := confirm()
		if !confirmed {
			logger.PrintInfo("Exiting.", nil)
			return
		}
	}

	db, err := openDB(dsn)
	if err != nil {
		logger.PrintFatal(err, nil)
	}
	defer db.Close()
	logger.PrintInfo("database connection pool established", nil)

	err = runMigrations(db)
	if err != nil {
		logger.PrintFatal(err, nil)
	}
	logger.PrintInfo("successfully migrated DB", nil)
}

func confirm() bool {
	reader := bufio.NewReader(os.Stdin)
	var answer bool

	for {
		fmt.Printf("\nAre you sure you want to initialize the database at DSN %s?\n\n**************************************************\n** WARNING: The target database should be empty **\n**************************************************\n\nRespnd Y or N -> ", dsn)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare("Y", strings.ToUpper(text)) == 0 {
			answer = true
			break
		} else if strings.Compare("N", strings.ToUpper(text)) == 0 {
			answer = false
			break
		} else {
			fmt.Println("Respond Y or N")
		}
	}

	return answer
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func runMigrations(db *sql.DB) error {
	_, err := db.Exec(createUsers)

	return err
}
