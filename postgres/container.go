package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type Container struct {
	databaseName string
	inMemory     bool
	hostPort     string
	dsn          string
	db           *sql.DB
	isTemplate   bool
	ccfg         *pgx.ConnConfig
	pool         *dockertest.Pool
	resource     *dockertest.Resource

	mu     sync.Mutex
	closed bool
}

type startConfig struct {
	databaseName string
	inMemory     bool
	timeout      time.Duration
	isTemplate   bool
	postStart    []postStartFunc
}

type startConfigFunc func(*startConfig)

type postStartFunc func(*Container) error

func WithDatabaseName(databaseName string) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.databaseName = databaseName
	}
}

func WithInMemory(inMemory bool) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.inMemory = inMemory
	}
}

func WithTimeout(timeout time.Duration) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.timeout = timeout
	}
}

func WithIsTemplate(isTemplate bool) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.isTemplate = isTemplate
	}
}

// WithPostStart adds a post-startup operation to the container.
// This can be used to install extensions, create tables, seed data etc.
func WithPostStart(funcs ...postStartFunc) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.postStart = funcs
	}
}

// Start a PostgreSQL container.
func Start(tb testing.TB, options ...startConfigFunc) *Container {
	tb.Helper()

	startCfg := startConfig{
		databaseName: "integrationtest",
	}
	for _, o := range options {
		o(&startCfg)
	}

	timeout := startCfg.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	c := &Container{
		databaseName: startCfg.databaseName,
		dsn:          "",
		inMemory:     startCfg.inMemory,
		isTemplate:   startCfg.isTemplate,
		hostPort:     "",
		db:           nil,
		ccfg:         nil,
	}

	var err error
	c.pool, err = dockertest.NewPool("")
	if err != nil {
		tb.Fatalf("unable to connect to Docker: %v", err)
	}
	if err = c.pool.Client.Ping(); err != nil {
		tb.Fatalf(`could not connect to docker: %v`, err)
	}

	env := []string{
		fmt.Sprintf("POSTGRES_DB=%s", c.databaseName),
		"POSTGRES_USER=postgres",
		"POSTGRES_PASSWORD=postgres",
		"listen_addresses = '*'",
	}
	if startCfg.inMemory {
		env = append(env, "PGDATA=/data")
	}

	c.resource, err = c.pool.RunWithOptions(&dockertest.RunOptions{
		Name:       fmt.Sprintf("%s_%09d", c.databaseName, time.Now().UnixNano()),
		Repository: "postgres",
		Tag:        "16-alpine",
		Env:        env,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.NeverRestart()

		if startCfg.inMemory {
			config.Tmpfs = map[string]string{
				"/data": "",
			}
		}
	})
	if err != nil {
		tb.Fatalf("unable to start PostgreSQL container: %v", err)
	}
	tb.Cleanup(func() {
		c.Close()
	})

	// Tell docker to hard kill the container in "timeout" seconds
	if err := c.resource.Expire(uint(timeout.Seconds())); err != nil {
		tb.Fatal(err)
	}
	c.pool.MaxWait = timeout

	c.hostPort = c.resource.GetHostPort("5432/tcp")

	c.dsn = fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", c.hostPort, c.databaseName)
	c.ccfg, err = pgx.ParseConfig(c.dsn)
	if err != nil {
		tb.Fatalf("could not parse connection string: %v", err)
	}

	// Configure logging from Docker container
	logWaiter, err := c.pool.Client.AttachToContainerNonBlocking(docker.AttachToContainerOptions{
		Container: c.resource.Container.ID,
		// OutputStream: os.Stdout,
		// ErrorStream:  os.Stderr,
		// Stderr:       true,
		// Stdout:       true,
		// Stream:       true,
	})
	if err != nil {
		tb.Fatalf("could not connect to PostgreSQL container log output: %v", err)
	}

	// Register cleanup process
	tb.Cleanup(func() {
		err = logWaiter.Close()
		if err != nil {
			tb.Fatalf("could not close container logs: %v", err)
		}
		err = logWaiter.Wait()
		if err != nil {
			tb.Fatalf("could not wait for container logs to close: %v", err)
		}
	})

	// Connect to PostgreSQL container
	err = c.pool.Retry(func() (err error) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		c.db, err = Connect(ctx, c.dsn)
		return
	})
	if err != nil {
		tb.Fatalf("could not connect to PostgreSQL container: %v", err)
	}

	// Run all post-startup operations
	for _, f := range startCfg.postStart {
		err = f(c)
		if err != nil {
			tb.Fatalf("could not run post-startup operation: %v", err)
		}
	}

	// Make it a template database?
	if c.isTemplate {
		sql := fmt.Sprintf(`UPDATE pg_database SET datistemplate = TRUE WHERE datname = '%s'`,
			pgx.Identifier([]string{c.databaseName}).Sanitize())
		_, err := c.db.Exec(sql)
		if err != nil {
			tb.Fatalf("could not make database a template: %v", err)
		}
	}

	return c
}

func (c *Container) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.isTemplate {
		sql := fmt.Sprintf(`UPDATE pg_database SET datistemplate = FALSE WHERE datname = '%s'`,
			pgx.Identifier([]string{c.databaseName}).Sanitize())
		_, err := c.db.Exec(sql)
		if err != nil {
			return fmt.Errorf("could not make database a template: %w", err)
		}
	}

	err := c.pool.Purge(c.resource)
	if err != nil {
		return fmt.Errorf("could not purge containers: %w", err)
	}

	c.closed = true

	return c.db.Close()
}

func (c *Container) DB() *sql.DB {
	return c.db
}

func (c *Container) ConnConfig() *pgx.ConnConfig {
	return c.ccfg
}

func (c *Container) StartFromTemplate(tb testing.TB) (*sql.DB, *pgx.ConnConfig, func() error) {
	if !c.isTemplate {
		tb.Fatal("cannot clone a non-template database: use WithIsTemplate(true) to create a template database")
	}

	databaseName := fmt.Sprintf("%s_%09d", c.databaseName, time.Now().UnixNano())
	sql := `CREATE DATABASE ` +
		pgx.Identifier([]string{databaseName}).Sanitize() +
		` TEMPLATE ` +
		pgx.Identifier([]string{c.databaseName}).Sanitize()
	_, err := c.db.Exec(sql)
	if err != nil {
		tb.Fatalf("could not make database a template: %v", err)
	}

	// Connect to cloned database
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", c.hostPort, c.databaseName)
	ccfg, err := pgx.ParseConfig(c.dsn)
	if err != nil {
		tb.Fatalf("could not parse connection string: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	db, err := Connect(ctx, dsn)
	if err != nil {
		tb.Fatalf("could not connect to PostgreSQL container: %v", err)
	}

	return db, ccfg, func() error {
		return db.Close()
	}
}
