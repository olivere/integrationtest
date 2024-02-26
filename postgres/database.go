package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// ConnectionString builds the connection string from the individual
// components.
func ConnectionString(host string, port uint16, name, sslMode, user, pass string) string {
	var uri url.URL
	uri.Scheme = "postgres"
	uri.User = url.UserPassword(user, pass)
	uri.Host = net.JoinHostPort(host, fmt.Sprint(port))
	uri.Path = name
	v := url.Values{}
	if sslMode != "" {
		v.Set("sslmode", sslMode)
	}
	if len(v) > 0 {
		uri.RawQuery = v.Encode()
	}
	return uri.String()
}

// Connect to a PostgreSQL server and connection check.
func Connect(ctx context.Context, databaseURL string) (*sql.DB, error) {
	c, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	db := stdlib.OpenDB(*c)

	// Ping
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

// DatabaseExists checks if the given database on a PostgreSQL server does
// exist.
func DatabaseExists(ctx context.Context, databaseURL string) (bool, error) {
	o, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return false, err
	}

	dsn := ConnectionString(o.Host, o.Port, "postgres", o.RuntimeParams["sslmode"], o.User, o.Password)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return false, err
	}

	if o.Database == "" {
		return false, errors.New("database name is empty")
	}

	db := stdlib.OpenDB(*cfg)

	// Borrowed from SQL Alchemy
	// See https://sqlalchemy-utils.readthedocs.io/en/latest/_modules/sqlalchemy_utils/functions/database.html#database_exists
	var n int64
	err = db.QueryRowContext(
		ctx,
		"SELECT 1 FROM pg_database WHERE datname=$1", o.Database,
	).Scan(&n)
	if IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// CreateDatabaseIfNotExists creates a PostgrSQL database if it doesn't
// already exist.
func CreateDatabaseIfNotExists(ctx context.Context, databaseURL string) (bool, error) {
	o, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return false, err
	}

	dsn := ConnectionString(o.Host, o.Port, "postgres", o.RuntimeParams["sslmode"], o.User, o.Password)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return false, err
	}

	if o.Database == "" {
		return false, errors.New("database name is empty")
	}

	db := stdlib.OpenDB(*cfg)

	// Borrowed from SQL Alchemy
	// See https://sqlalchemy-utils.readthedocs.io/en/latest/_modules/sqlalchemy_utils/functions/database.html#create_database
	sql := "CREATE DATABASE " + pgx.Identifier([]string{o.Database}).Sanitize()
	_, err = db.ExecContext(ctx, sql)
	if IsDupDB(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// DropDatabaseIfExists drops a PostgrSQL database if it exist.
func DropDatabaseIfExists(ctx context.Context, databaseURL string) (bool, error) {
	o, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return false, err
	}

	dsn := ConnectionString(o.Host, o.Port, "postgres", o.RuntimeParams["sslmode"], o.User, o.Password)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return false, err
	}

	if o.Database == "" {
		return false, errors.New("database name is empty")
	}

	db := stdlib.OpenDB(*cfg)

	sql := "DROP DATABASE " + pgx.Identifier([]string{o.Database}).Sanitize()
	_, err = db.ExecContext(ctx, sql)
	if IsDBNotExists(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
