package engine

import (
	"context"
	"starless/kadath/configs"
)

// Engine defines the common interface that all database engines must implement
type Engine interface {
	// Ping checks if the database is reachable
	Ping(ctx context.Context) error

	// Close closes the database connection
	Close() error
}

// NewEngine creates a new engine instance based on the build tag
// This function will be implemented by each engine-specific file
func NewEngine(cfg *configs.Config) (Engine, error) {
	return newEngine(cfg)
}
