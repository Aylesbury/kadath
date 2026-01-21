//go:build mysql

package engine

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"starless/kadath/configs"
)

type mysqlEngine struct {
	db *sql.DB
}

func newEngine(cfg configs.Config) (Engine, error) {
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	return &mysqlEngine{
		db: db,
	}, nil
}

func (e *mysqlEngine) Ping(ctx context.Context) error {
	// Use SELECT 1 to check if database is reachable
	var result int
	err := e.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("mysql ping failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("mysql ping returned unexpected result: %d", result)
	}

	return nil
}

func (e *mysqlEngine) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
