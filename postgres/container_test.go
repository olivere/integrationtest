package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/olivere/integrationtest/postgres"
)

func TestContainer_Start(t *testing.T) {
	c := postgres.Start(t, postgres.WithTimeout(10*time.Second))
	defer c.Close()

	// Ping database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err != nil {
		t.Fatalf("could not ping database: %v", err)
	}

	// Stop container
	if err := c.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}

	// Ping database
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestContainer_PostStart(t *testing.T) {
	c := postgres.Start(t,
		postgres.WithTimeout(10*time.Second),
		postgres.WithPostStart(func(c *postgres.Container) error {
			// Create UUID extension
			_, err := c.DB().Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
			return err
		}, func(c *postgres.Container) error {
			// Add a table
			_, err := c.DB().Exec(`CREATE TABLE foo (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				name TEXT
			)`)
			return err
		}),
	)
	defer c.Close()

	// Ping database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err != nil {
		t.Fatalf("could not ping database: %v", err)
	}

	// Check if UUID extension exists by adding a foo entity
	_, err := c.DB().Exec("INSERT INTO foo (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("could not insert into foo: %v", err)
	}
	type foo struct {
		ID   string
		Name string
	}
	var f foo
	err = c.DB().QueryRow("SELECT id, name FROM foo").Scan(&f.ID, &f.Name)
	if err != nil {
		t.Fatalf("could not query foo: %v", err)
	}
	if f.ID == "" {
		t.Fatalf("expected non-empty ID, got empty")
	}
	if f.Name != "test" {
		t.Fatalf("expected name=%q, got %q", "test", f.Name)
	}

	// Stop container
	if err := c.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}

	// Ping database
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

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

func TestContainer_StartFromTemplate(t *testing.T) {
	c := postgres.Start(t, postgres.WithTimeout(10*time.Second), postgres.WithIsTemplate(true))
	defer c.Close()

	// Ping database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err != nil {
		t.Fatalf("could not ping database: %v", err)
	}

	// Clone the database
	db, cfg, dbclose := c.StartFromTemplate(t)
	defer dbclose()

	if cfg.Database == "" {
		t.Fatalf("expected non-empty database name, got empty")
	}

	// Ping cloned database
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("could not ping cloned database: %v", err)
	}

	// Stop cloned database
	if err := db.Close(); err != nil {
		t.Fatalf("could not stop cloned database: %v", err)
	}

	// Stop container
	if err := c.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}

	// Ping database
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DB().PingContext(ctx); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func Scoped(tb testing.TB, db *sql.DB, role, tenant string, f func(tx *sql.Tx) error) {
	tb.Helper()

	tx, err := db.Begin()
	if err != nil {
		tb.Fatalf("could not begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(fmt.Sprintf("SET ROLE %s", role))
	if err != nil {
		tb.Fatalf("could not set role: %v", err)
	}

	_, err = tx.Exec(fmt.Sprintf("SET current.tenant = '%s'", tenant))
	if err != nil {
		tb.Fatalf("could not set current.tenant: %v", err)
	}

	if err := f(tx); err != nil {
		tb.Fatalf("could not run scoped function: %v", err)
	}

	_, err = tx.Exec("RESET current.tenant")
	if err != nil {
		tb.Fatalf("could not set current.tenant: %v", err)
	}

	_, err = tx.Exec("RESET ROLE")
	if err != nil {
		tb.Fatalf("could not set role: %v", err)
	}

	if err := tx.Commit(); err != nil {
		tb.Fatalf("could not commit transaction: %v", err)
	}
}

func TestContainer_StartFromTemplate_WithRolesAndRLS(t *testing.T) {
	c := postgres.Start(t,
		postgres.WithTimeout(10*time.Second),
		postgres.WithIsTemplate(true),
		postgres.WithPostStart(func(c *postgres.Container) error {
			tx, err := c.DB().Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			if _, err := tx.Exec(`
			CREATE ROLE manager NOLOGIN;
			GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO manager;
			ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO manager;

			CREATE ROLE student NOLOGIN;
			GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO student;
			ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO student;

			CREATE TABLE foo (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				tenant TEXT NOT NULL,
				name TEXT NOT NULL
			);

			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;

			CREATE POLICY foo_manager_policy ON foo
				FOR ALL
				TO manager
				USING (true);

			CREATE POLICY foo_student_policy ON foo
				FOR SELECT
				TO student
				USING (tenant = current_setting('current.tenant')::TEXT);

			INSERT INTO foo (tenant, name) VALUES ('one', 'foo1');
			INSERT INTO foo (tenant, name) VALUES ('one', 'foo2');
			INSERT INTO foo (tenant, name) VALUES ('two', 'foo3');

			`); err != nil {
				return err
			}

			return tx.Commit()
		}),
	)
	defer c.Close()

	// Clone the database
	db, cfg, dbclose := c.StartFromTemplate(t)
	defer dbclose()

	if cfg.Database == "" {
		t.Fatalf("expected non-empty database name, got empty")
	}

	// Run a query as manager, and check if RLS is applied
	Scoped(t, db, "manager", "one", func(tx *sql.Tx) error {
		// Managers can read all rows from foo, so that should be 3
		var n int
		err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
		if err != nil {
			t.Fatalf("could not query foo: %v", err)
		}
		if want, have := 3, n; want != have {
			t.Fatalf("want n=%d, have %d", want, have)
		}
		return nil
	})

	// Run a query as student and check if RLS is applied
	Scoped(t, db, "student", "one", func(tx *sql.Tx) error {
		// Students can only read rows from foo where tenant is set to
		// current.tenant, so that should only be 2
		var n int
		err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
		if err != nil {
			t.Fatalf("could not query foo: %v", err)
		}
		if want, have := 2, n; want != have {
			t.Fatalf("want n=%d, have %d", want, have)
		}
		return nil
	})

	// Stop cloned database
	if err := db.Close(); err != nil {
		t.Fatalf("could not stop cloned database: %v", err)
	}

	// Stop container
	if err := c.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}
}

func BenchmarkContainer_Default(b *testing.B) {
	// Clone the database
	for i := 0; i < b.N; i++ {
		c := postgres.Start(b,
			postgres.WithTimeout(10*time.Second),
			postgres.WithInMemory(false),
			postgres.WithIsTemplate(false),
			postgres.WithPostStart(func(c *postgres.Container) error {
				tx, err := c.DB().Begin()
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if _, err := tx.Exec(`
					CREATE ROLE manager NOLOGIN;
					GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO manager;
					ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO manager;

					CREATE ROLE student NOLOGIN;
					GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO student;
					ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO student;

					CREATE TABLE foo (
						id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
						tenant TEXT NOT NULL,
						name TEXT NOT NULL
					);

					ALTER TABLE foo ENABLE ROW LEVEL SECURITY;

					CREATE POLICY foo_manager_policy ON foo
						FOR ALL
						TO manager
						USING (true);

					CREATE POLICY foo_student_policy ON foo
						FOR SELECT
						TO student
						USING (tenant = current_setting('current.tenant')::TEXT);

					INSERT INTO foo (tenant, name) VALUES ('one', 'foo1');
					INSERT INTO foo (tenant, name) VALUES ('one', 'foo2');
					INSERT INTO foo (tenant, name) VALUES ('two', 'foo3');

					`); err != nil {
					return err
				}

				return tx.Commit()
			}),
		)

		db := c.DB()

		if rand.Int31n(2) == 0 {
			// Run a query as manager, and check if RLS is applied
			Scoped(b, db, "manager", "one", func(tx *sql.Tx) error {
				// Managers can read all rows from foo, so that should be 3
				var n int
				err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
				if err != nil {
					b.Fatalf("could not query foo: %v", err)
				}
				if want, have := 3, n; want != have {
					b.Fatalf("want n=%d, have %d", want, have)
				}
				return nil
			})
		} else {
			// Run a query as student and check if RLS is applied
			Scoped(b, db, "student", "one", func(tx *sql.Tx) error {
				// Students can only read rows from foo where tenant is set to
				// current.tenant, so that should only be 2
				var n int
				err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
				if err != nil {
					b.Fatalf("could not query foo: %v", err)
				}
				if want, have := 2, n; want != have {
					b.Fatalf("want n=%d, have %d", want, have)
				}
				return nil
			})
		}

		if err := c.Close(); err != nil {
			b.Fatalf("could not stop container: %v", err)
		}
	}
}

func BenchmarkContainer_Template(b *testing.B) {
	c := postgres.Start(b,
		postgres.WithTimeout(10*time.Second),
		postgres.WithInMemory(true),
		postgres.WithIsTemplate(true),
		postgres.WithPostStart(func(c *postgres.Container) error {
			tx, err := c.DB().Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			if _, err := tx.Exec(`
			CREATE ROLE manager NOLOGIN;
			GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO manager;
			ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO manager;

			CREATE ROLE student NOLOGIN;
			GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO student;
			ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO student;

			CREATE TABLE foo (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				tenant TEXT NOT NULL,
				name TEXT NOT NULL
			);

			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;

			CREATE POLICY foo_manager_policy ON foo
				FOR ALL
				TO manager
				USING (true);

			CREATE POLICY foo_student_policy ON foo
				FOR SELECT
				TO student
				USING (tenant = current_setting('current.tenant')::TEXT);

			INSERT INTO foo (tenant, name) VALUES ('one', 'foo1');
			INSERT INTO foo (tenant, name) VALUES ('one', 'foo2');
			INSERT INTO foo (tenant, name) VALUES ('two', 'foo3');

			`); err != nil {
				return err
			}

			return tx.Commit()
		}),
	)
	defer c.Close()

	for i := 0; i < b.N; i++ {
		// Clone the database
		db, _, dbclose := c.StartFromTemplate(b)

		if rand.Int31n(2) == 0 {
			// Run a query as manager, and check if RLS is applied
			Scoped(b, db, "manager", "one", func(tx *sql.Tx) error {
				// Managers can read all rows from foo, so that should be 3
				var n int
				err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
				if err != nil {
					b.Fatalf("could not query foo: %v", err)
				}
				if want, have := 3, n; want != have {
					b.Fatalf("want n=%d, have %d", want, have)
				}
				return nil
			})
		} else {
			// Run a query as student and check if RLS is applied
			Scoped(b, db, "student", "one", func(tx *sql.Tx) error {
				// Students can only read rows from foo where tenant is set to
				// current.tenant, so that should only be 2
				var n int
				err := tx.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n)
				if err != nil {
					b.Fatalf("could not query foo: %v", err)
				}
				if want, have := 2, n; want != have {
					b.Fatalf("want n=%d, have %d", want, have)
				}
				return nil
			})
		}

		dbclose()
	}

	// Stop container
	if err := c.Close(); err != nil {
		b.Fatalf("could not stop container: %v", err)
	}
}
