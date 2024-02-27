package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/olivere/integrationtest/postgres"
)

func TestDatabaseManagement(t *testing.T) {
	c := postgres.Start(t, postgres.WithTimeout(10*time.Second))
	defer c.Close()

	cfg := c.ConnConfig()

	// Database should not exist here
	connString := postgres.ConnectionString(cfg.Host, cfg.Port, "new-database", "", cfg.User, cfg.Password)
	exists, err := postgres.DatabaseExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := false, exists; want != have {
		t.Fatalf("want Exists=%v, have %v", want, have)
	}

	// Database should be created here
	created, err := postgres.CreateDatabaseIfNotExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("want Created=%v, have %v", true, created)
	}

	// Database should exist now
	exists, err = postgres.DatabaseExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if want, have := true, exists; want != have {
		t.Fatalf("want Exists=%v, have %v", want, have)
	}

	// Recreating the database should be a no-op
	created, err = postgres.CreateDatabaseIfNotExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatalf("want Created=%v, have %v", false, created)
	}

	// Drop the database
	dropped, err := postgres.DropDatabaseIfExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if !dropped {
		t.Fatalf("want Dropped=%v, have %v", true, dropped)
	}

	// Drop the database (again)
	dropped, err = postgres.DropDatabaseIfExists(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	if dropped {
		t.Fatalf("want Dropped=%v, have %v", false, dropped)
	}
}
