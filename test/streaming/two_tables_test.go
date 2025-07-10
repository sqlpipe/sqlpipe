package test

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func createPostgresqlProductsAndPricesTables(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
			   CREATE TABLE products (
					   id TEXT PRIMARY KEY,
					   name TEXT NOT NULL,
					   default_price_id TEXT UNIQUE,
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

	_, err = db.Exec(`
			   CREATE TABLE prices (
					   id text PRIMARY KEY,
					   product_id TEXT NOT NULL,
					   unit_amount INT NOT NULL,
					   currency TEXT NOT NULL
			   );
	   `)
	if err != nil {
		t.Fatalf("Failed to create prices table: %v", err)
	}
}

func TestTwoTablesStreaming(t *testing.T) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	pool.MaxWait = 20 * time.Second

	postgresqlPassword := "Mypass123"
	postgresqlUsername := "postgres"
	postgresqlDatabase := "postgres"

	// Resource handles for cleanup
	var (
		network             *dockertest.Network
		postgresqlContainer *dockertest.Resource
		sqlpipeContainer    *dockertest.Resource
	)

	// Setup signal handler for cleanup with improved error handling
	cleanup := func() {
		// Helper to check for specific Docker errors
		isAlreadyRemoving := func(err error) bool {
			return err != nil && (strings.Contains(err.Error(), "removal of container") && strings.Contains(err.Error(), "is already in progress"))
		}
		isNetworkActive := func(err error) bool {
			return err != nil && (strings.Contains(err.Error(), "network") && strings.Contains(err.Error(), "has active endpoints"))
		}

		if sqlpipeContainer != nil {
			if err := pool.Purge(sqlpipeContainer); err != nil && !isAlreadyRemoving(err) {
				log.Printf("Could not purge sqlpipe resource: %s", err)
			}
		}
		if postgresqlContainer != nil {
			if err := pool.Purge(postgresqlContainer); err != nil && !isAlreadyRemoving(err) {
				log.Printf("Could not purge resource: %s", err)
			}
		}
		if network != nil {
			// Retry network removal if it has active endpoints
			for i := 0; i < 5; i++ {
				if err := pool.RemoveNetwork(network); err != nil {
					if isNetworkActive(err) {
						time.Sleep(1 * time.Second)
						continue
					}
					log.Printf("Could not remove docker network: %s", err)
					break
				}
				break // success
			}
		}
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		log.Println("Interrupt received, cleaning up Docker resources...")
		cleanup()
		os.Exit(1)
	}()

	defer cleanup()

	// Create a network for both containers
	network, err = pool.CreateNetwork("sqlpipe-test-network")
	if err != nil {
		t.Fatalf("Could not create docker network: %s", err)
	}

	postgresqlContainer, err = pool.BuildAndRunWithOptions("../../postgresql.dockerfile", &dockertest.RunOptions{
		Name: "test-postgres",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%v", postgresqlUsername),
			fmt.Sprintf("POSTGRES_PASSWORD=%v", postgresqlPassword),
			fmt.Sprintf("POSTGRES_DB=%v", postgresqlDatabase),
		},
		NetworkID:    network.Network.ID,
		ExposedPorts: []string{"5432/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {{HostIP: "0.0.0.0", HostPort: "5432"}},
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=5",
			"-c", "max_wal_senders=5",
			"-c", "max_connections=100",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
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

	createPostgresqlProductsAndPricesTables(t, db)

	_, err = db.Exec(`CREATE PUBLICATION my_pub FOR ALL TABLES;`)
	if err != nil {
		t.Fatalf("Failed to create publication: %v", err)
	}

	buildCmd := exec.Command("go", []string{"build", "-o", "../../bin/streaming", "../../cmd/streaming"}...)
	buildCmd.Env = append(os.Environ(),
		"GOOS=linux",
		fmt.Sprintf("GOARCH=%v", runtime.GOARCH),
		"CGO_ENABLED=0",
	)

	// buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build streaming app: %v", err)
	}

	systemsHostDir, err := filepath.Abs("./config/two-tables/systems")
	if err != nil {
		t.Fatalf("Failed to get absolute path for systems config: %v", err)
	}
	if _, err := os.Stat(systemsHostDir); os.IsNotExist(err) {
		t.Fatalf("Systems config directory does not exist: %s", systemsHostDir)
	}

	modelsHostDir, err := filepath.Abs("./config/two-tables/models")
	if err != nil {
		t.Fatalf("Failed to get absolute path for models config: %v", err)
	}
	if _, err := os.Stat(modelsHostDir); os.IsNotExist(err) {
		t.Fatalf("Models config directory does not exist: %s", modelsHostDir)
	}

	sqlpipeContainer, err = pool.BuildAndRunWithOptions("../../streaming.dockerfile", &dockertest.RunOptions{
		Name: "sqlpipe-streaming",
		Env: []string{
			"PORT=4000",
			"SYSTEMS_DIR=/config/two-tables/systems",
			"MODELS_DIR=/config/two-tables/models",
		},
		Mounts: []string{
			fmt.Sprintf("%s:/config/two-tables/systems", systemsHostDir),
			fmt.Sprintf("%s:/config/two-tables/models", modelsHostDir),
		},
		NetworkID: network.Network.ID,
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	go func() {
		pool.Client.Logs(docker.LogsOptions{
			Container:    sqlpipeContainer.Container.ID,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Follow:       true,
			Stdout:       true,
			Stderr:       true,
		})
	}()

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
		t.Fatalf("SQLpipe healthcheck failed: %v", err)
	}

	fmt.Println("SQLpipe is running and healthy!")

	time.Sleep(1 * time.Second)

	stripeCmd := exec.Command("stripe", "trigger", "product.created")
	stripeCmd.Stdout = os.Stdout
	stripeCmd.Stderr = os.Stderr
	fmt.Println("stripe api key: ", os.Getenv("STRIPE_API_KEY"))
	stripeCmd.Env = append(os.Environ(), fmt.Sprintf("STRIPE_API_KEY=%s", os.Getenv("STRIPE_API_KEY")))
	err = stripeCmd.Run()
	if err != nil {
		t.Fatalf("Failed to run stripe trigger: %v", err)
	}

	fmt.Println("Test is running. Press Ctrl+C to exit.")
	select {}
}
