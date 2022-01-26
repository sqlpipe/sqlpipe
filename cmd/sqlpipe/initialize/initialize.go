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
	InitializeCmd = &cobra.Command{
		Use:   "initialize",
		Short: "Initialize PostgreSQL DB.",
		Run:   initialize,
	}

	dsn   string
	force bool

	createUsers = `
		CREATE TABLE users (
			id bigserial PRIMARY KEY,
			created_at timestamp(0) NOT NULL DEFAULT NOW(),
			username text UNIQUE NOT NULL,
			password_hash bytea NOT NULL,
			admin bool NOT NULL DEFAULT false,
			version INT NOT NULL DEFAULT 1
		);
	`

	createConnections = `
		CREATE TABLE connections (
			id bigserial PRIMARY KEY,
			created_at timestamp(0) NOT NULL DEFAULT NOW(),
			name text UNIQUE NOT NULL,
			ds_type text not null,
			username TEXT NOT NULL,
			password TEXT NOT NULL,
			account_id TEXT NOT NULL DEFAULT '',
			hostname TEXT NOT NULL DEFAULT '',
			port INT NOT NULL DEFAULT 0,
			db_name TEXT NOT NULL,
			version INT NOT NULL DEFAULT 1
		);
	`

	createTransfers = `
		CREATE TABLE transfers (
			id bigserial PRIMARY KEY,
			created_at timestamp(0) NOT NULL DEFAULT NOW(),
			source_id bigint not null,
			target_id bigint not null,
			query text not null,
			target_schema text not null,
			target_table text not null,
			overwrite bool not null,
			status text not null default 'queued',
			error text not null default '',
			error_properties text not null default '',
			stopped_at timestamp(0) not null,
			Version int not null default 1,
			FOREIGN KEY (source_id) REFERENCES connections(id),
			FOREIGN KEY (target_id) REFERENCES connections(id)
		);
	`

	createQueries = `
	CREATE TABLE queries (
		id bigserial PRIMARY KEY,
		created_at timestamp(0) NOT NULL DEFAULT NOW(),
		connection_id bigint not null,
		query text not null,
		status text not null default 'queued',
		error text not null default '',
		error_properties text not null default '',
		stopped_at timestamp(0) not null,
		Version int not null default 1,
		FOREIGN KEY (connection_id) REFERENCES connections(id)
	);
`
)

func init() {
	InitializeCmd.Flags().StringVar(&dsn, "dsn", "", "Database backend connection string")
	InitializeCmd.Flags().BoolVar(&force, "force", false, "Do not ask for confirmation")
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

	runMigrations(db)
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
	if err != nil {
		fmt.Println("Error running migrations on users table:")
		fmt.Println(err)
		os.Exit(1)
	}
	_, err = db.Exec(createConnections)
	if err != nil {
		fmt.Println("Error running migrations on connections table:")
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = db.Exec(createTransfers)
	if err != nil {
		fmt.Println("Error running migrations on transfers table:")
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = db.Exec(createQueries)
	if err != nil {
		fmt.Println("Error running migrations on queries table:")
		fmt.Println(err)
		os.Exit(1)
	}

	return err
}
