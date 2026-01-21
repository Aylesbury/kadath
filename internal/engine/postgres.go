//go:build postgres

package engine

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"starless/kadath/configs"
)

type postgresEngine struct {
	db *sql.DB
}

func newEngine(cfg configs.Config) (Engine, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	return &postgresEngine{
		db: db,
	}, nil
}

func (e *postgresEngine) Ping(ctx context.Context) error {
	// Use SELECT 1 to check if database is reachable
	var result int
	err := e.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("postgres ping returned unexpected result: %d", result)
	}

	return nil
}

func (e *postgresEngine) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
