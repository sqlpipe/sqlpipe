package test

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func createPostgresqlProductsTable(t *testing.T, db *sql.DB) {
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

func TestStreaming(t *testing.T) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	pool.MaxWait = 10 * time.Second

	postgresqlPassword := "Mypass123"
	postgresqlUsername := "postgres"
	postgresqlDatabase := "postgres"

	// Create a network for both containers
	network, err := pool.CreateNetwork("sqlpipe-test-network")
	defer func() {
		if err := pool.RemoveNetwork(network); err != nil {
			log.Printf("Could not remove docker network: %s", err)
		}
	}()
	if err != nil {
		t.Fatalf("Could not create docker network: %s", err)
	}

	postgresqlContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "debezium/postgres",
		Tag:        "15",
		Name:       "test-postgres",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%v", postgresqlUsername),
			fmt.Sprintf("POSTGRES_PASSWORD=%v", postgresqlPassword),
			fmt.Sprintf("POSTGRES_DB=%v", postgresqlDatabase),
		},
		NetworkID: network.Network.ID,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	defer func() {
		if postgresqlContainer != nil {
			if err := pool.Purge(postgresqlContainer); err != nil {
				t.Fatalf("Could not purge resource: %s", err)
			}
		}
	}()
	if err != nil {
		t.Fatalf("Could not start PostgreSQL: %s", err)
	}

	var db *sql.DB
	if err := pool.Retry(func() error {
		var err error
		port := postgresqlContainer.GetPort("5432/tcp")
		dsn := fmt.Sprintf("postgres://%v:%v@localhost:%s/%v?sslmode=disable", postgresqlUsername, postgresqlPassword, port, postgresqlDatabase)
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}

	createPostgresqlProductsTable(t, db)

	buildCmd := exec.Command("go", []string{"build", "-o", "../../bin/streaming", "../../cmd/streaming"}...)
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
			"STRIPE_LISTEN=true",
			fmt.Sprintf("STRIPE_API_KEY=%s", os.Getenv("STRIPE_API_KEY")),
		},
		Mounts: []string{
			fmt.Sprintf("%s:/config/systems", systemsHostDir),
			fmt.Sprintf("%s:/config/models", modelsHostDir),
		},
		NetworkID: network.Network.ID,
	})
	defer func() {
		if sqlpipeContainer != nil {
			if err := pool.Purge(sqlpipeContainer); err != nil {
				t.Fatalf("Could not purge sqlpipe resource: %s", err)
			}
		}
	}()
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	err = pool.Retry(func() error {

		inspect, err := pool.Client.InspectContainer(sqlpipeContainer.Container.ID)
		if err != nil {
			return fmt.Errorf("failed to inspect container: %w", err)
		}
		if !inspect.State.Running {
			return fmt.Errorf("container exited with code: %d", inspect.State.ExitCode)
		}

		hostPort := sqlpipeContainer.GetPort("4000/tcp")
		healthcheckURL := fmt.Sprintf("http://localhost:%s/v1/healthcheck", hostPort)

		resp, err := http.Get(healthcheckURL)
		if err != nil {
			return fmt.Errorf("healthcheck error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("healthcheck returned status %d", resp.StatusCode)
		}
		return nil // success!
	})
	if err != nil {
		pool.Client.Logs(docker.LogsOptions{
			Container:    sqlpipeContainer.Container.ID,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Follow:       false,
			Stdout:       true,
			Stderr:       true,
		})
		t.Fatalf("SQLpipe healthcheck failed: %v", err)
	}

	stripeCmd := exec.Command("stripe", "trigger", "payment_intent.succeeded")
	stripeCmd.Stdout = os.Stdout
	stripeCmd.Stderr = os.Stderr
	stripeCmd.Env = append(os.Environ(), "STRIPE_API_KEY="+os.Getenv("STRIPE_API_KEY"))
	if err := stripeCmd.Run(); err != nil {
		t.Fatalf("Failed to run stripe trigger: %v", err)
	}

	pool.Client.Logs(docker.LogsOptions{
		Container:    sqlpipeContainer.Container.ID,
		OutputStream: os.Stdout,
		ErrorStream:  os.Stderr,
		Follow:       true,
		Stdout:       true,
		Stderr:       true,
	})

}
