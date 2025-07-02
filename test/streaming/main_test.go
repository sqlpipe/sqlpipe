package test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func TestStreaming(t *testing.T) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	postgresqlContainer, db := postgresqlSetup(t, pool)

	sqlpipeContainer := sqlpipeSetup(t, pool)

	defer func() {
		if err := pool.Purge(sqlpipeContainer); err != nil {
			log.Fatalf("Could not purge sqlpipe resource: %s", err)
		}
	}()

	// Clean up container at the end
	defer func() {
		if err := pool.Purge(postgresqlContainer); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	createProductsTable(t, db)
}

// sqlpipeSetup builds and runs the SQLPipe container and streams logs to stdout.
func sqlpipeSetup(t *testing.T, pool *dockertest.Pool) *dockertest.Resource {

	cmd := "go"
	args := []string{"build", "-o", "../../bin/streaming", "../../cmd/streaming"}

	buildCmd := exec.Command(cmd, args...)

	buildCmd.Env = append(os.Environ(),
		"GOOS=linux",
		fmt.Sprintf("GOARCH=%v", runtime.GOARCH),
		"CGO_ENABLED=0",
	)

	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build streaming app: %v", err)
	}

	systemsHostDir, err := filepath.Abs("./config/systems")
	if err != nil {
		t.Fatalf("Failed to get absolute path for systems config: %v", err)
	}
	if _, err := os.Stat(systemsHostDir); os.IsNotExist(err) {
		t.Fatalf("Systems config directory does not exist: %s", systemsHostDir)
	}

	modelsHostDir, err := filepath.Abs("./config/models")
	if err != nil {
		t.Fatalf("Failed to get absolute path for models config: %v", err)
	}
	if _, err := os.Stat(modelsHostDir); os.IsNotExist(err) {
		t.Fatalf("Models config directory does not exist: %s", modelsHostDir)
	}

	sqlpipeContainer, err := pool.BuildAndRunWithOptions("../../streaming.dockerfile", &dockertest.RunOptions{
		Name: "sqlpipe-streaming",
		Env: []string{
			"PORT=4000",
			"SYSTEMS_YAML_DIR=/config/systems",
			"MODELS_YAML_DIR=/config/models",
			"QUEUE_DIR=/tmp/sqlpipe/queue",
			"SEGMENT_SIZE=1000",
		},
		Mounts: []string{
			fmt.Sprintf("%s:/config/systems", systemsHostDir),
			fmt.Sprintf("%s:/config/models", modelsHostDir),
		},
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// Stream logs to stdout
	err = pool.Client.Logs(docker.LogsOptions{
		Container:    sqlpipeContainer.Container.ID,
		OutputStream: os.Stdout,
		Stdout:       true,
		Stderr:       true,
		Follow:       true,
	})
	if err != nil {
		t.Fatalf("Failed to get container logs: %v", err)
	}

	return sqlpipeContainer
}

// createProductsTable creates the products table in the given database.
func createProductsTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
			   CREATE TABLE products (
					   id TEXT PRIMARY KEY,
					   name TEXT NOT NULL,
					   active BOOLEAN DEFAULT TRUE,
					   created TIMESTAMPTZ,
					   updated TIMESTAMPTZ,
					   description TEXT,
					   livemode BOOLEAN DEFAULT FALSE,
					   statement_descriptor TEXT,
					   unit_label TEXT,
					   category TEXT,
					   internal_notes TEXT
			   );
	   `)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
}

// postgresqlSetup sets up a PostgreSQL container and returns the pool, container, and db connection.
func postgresqlSetup(t *testing.T, pool *dockertest.Pool) (*dockertest.Resource, *sql.DB) {

	postgresqlContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "debezium/postgres",
		Tag:        "15",
		Env: []string{
			// "POSTGRES_USER=postgres",
			"POSTGRES_PASSWORD=Mypass123",
			// "POSTGRES_DB=postgres",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start PostgreSQL: %s", err)
	}

	var db *sql.DB
	if err := pool.Retry(func() error {
		var err error
		port := postgresqlContainer.GetPort("5432/tcp")
		dsn := fmt.Sprintf("postgres://postgres:Mypass123@localhost:%s/postgres?sslmode=disable", port)
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	return postgresqlContainer, db
}
