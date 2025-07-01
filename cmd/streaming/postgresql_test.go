package main

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func TestPostgreSQL(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "debezium/postgres",
		Tag:        "15",
		Env: []string{
			"POSTGRES_USER=postgres",
			"POSTGRES_PASSWORD=Mypass123",
			"POSTGRES_DB=postgres",
		},
	}, func(config *docker.HostConfig) {
		// Allow container to be reused across test runs (optional)
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// Clean up container at the end
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	// Build connection string
	var db *sql.DB
	if err := pool.Retry(func() error {
		var err error
		port := resource.GetPort("5432/tcp")
		dsn := fmt.Sprintf("postgres://postgres:Mypass123@localhost:%s/postgres?sslmode=disable", port)
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	_, err = db.Exec(`
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

	build := exec.Command("go", "build", "-o", "streaming", "./")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build app: %v\nOutput:\n%s", err, string(out))
	}

	defer func() {
		if err := exec.Command("rm", "-f", "streaming").Run(); err != nil {
			t.Fatalf("Failed to clean up binary: %v", err)
		}
	}()

	cmd := exec.Command("./streaming", "-config", "sample.yaml")
	out, err := cmd.CombinedOutput()
	fmt.Printf("App output:\n%s\n", string(out))

	if err != nil {
		t.Fatalf("Failed to run app: %v", err)
	}

}
