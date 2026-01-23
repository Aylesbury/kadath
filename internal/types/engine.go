package types

import "context"

// Engine defines the common interface that all database engines must implement
type Engine interface {
	// Ping checks if the database is reachable
	Ping(ctx context.Context) error

	// ExecuteQuery executes a DSL query and returns results
	ExecuteQuery(ctx context.Context, params *QueryParams) (*QueryResponse, error)

	// Close closes the database connection
	Close() error
}
