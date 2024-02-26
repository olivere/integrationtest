package postgres_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/olivere/integrationtest/postgres"
)

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		Error    error
		Expected bool
	}{
		{Error: nil, Expected: false},
		{Error: sql.ErrNoRows, Expected: true},
		{Error: fmt.Errorf("kaboom: %w", sql.ErrNoRows), Expected: true},
		{Error: pgx.ErrNoRows, Expected: true},
		{Error: fmt.Errorf("kaboom: %w", pgx.ErrNoRows), Expected: true},
	}
	for i, tc := range tests {
		if want, have := tc.Expected, postgres.IsNotFound(tc.Error); want != have {
			t.Errorf("#%d: postgres.IsNotFound(%v): want %v, have %v", i, tc.Error, want, have)
		}
	}
}

func TestIsDup(t *testing.T) {
	tests := []struct {
		Error    error
		Expected bool
	}{
		{Error: nil, Expected: false},
		{Error: &pgconn.PgError{Code: "23505"}, Expected: true},
		{Error: fmt.Errorf("kaboom: %w", &pgconn.PgError{Code: "23505"}), Expected: true},
	}
	for i, tc := range tests {
		if want, have := tc.Expected, postgres.IsDup(tc.Error); want != have {
			t.Errorf("#%d: postgres.IsDup(%v): want %v, have %v", i, tc.Error, want, have)
		}
	}
}
