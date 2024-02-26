package postgres

import (
	"database/sql"
	stderrors "errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsNotFound returns true if the given error indicates that
// a record could not be found.
func IsNotFound(err error) bool {
	return stderrors.Is(err, sql.ErrNoRows) ||
		stderrors.Is(err, pgx.ErrNoRows)
}

// IsPSQLError returns true if the given error is from PostgreSQL and has the
// given error code.
//
// See https://www.postgresql.org/docs/13/errcodes-appendix.html
// for a list of all PostgreSQL error codes.
func IsPSQLError(err error, code string) bool {
	if err == nil {
		return false
	}
	var pgxerr *pgconn.PgError
	if stderrors.As(err, &pgxerr) {
		return pgxerr.Code == code
	}
	return false
}

// IsForeignKeyViolation returns true if the given error indicates a
// violation of a foreign key constraint (23503 foreign_key_violation).
func IsForeignKeyViolation(err error) bool {
	// 23503 foreign_key_violation
	return IsPSQLError(err, "23503")
}

// IsDup returns true if the given error indicates that a
// duplicate record has been found (23505 unique_violation).
func IsDup(err error) bool {
	// 23505 unique_violation
	return IsPSQLError(err, "23505")
}

// IsPerm returns true if the given error indicates a permission issue
// (42501 insufficient_privilege).
func IsPerm(err error) bool {
	// 42501 insufficient_privilege
	return IsPSQLError(err, "42501")
}

// IsDupDB returns true if the given error indicates the database already
// exists. This is typically returned from the `CREATE DATABASE dbname` command
// if `dbname` already exists (42P04 database "..." already exists).
func IsDupDB(err error) bool {
	// 42P04 database "..." already exists
	return IsPSQLError(err, "42P04")
}

// IsDBNotExists returns true if the given error indicates error code 3D000:
// database "..." does not exist.
func IsDBNotExists(err error) bool {
	// 3D000 database "..." does not exist
	return IsPSQLError(err, "3D000")
}

// IsDupUser returns true if the given error indicates the user/role already
// exists. This is typically returned from the `CREATE ROLE user ...` command
// if `user` already exists (42710 role "..." already exists).
func IsDupUser(err error) bool {
	// 42710 role "..." already exists
	return IsPSQLError(err, "42710")
}
