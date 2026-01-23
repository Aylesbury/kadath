package engine

import (
	"starless/kadath/configs"
	"starless/kadath/internal/types"
)

// NewEngine creates a new engine instance based on the build tag
// This function will be implemented by each engine-specific file
func NewEngine(cfg *configs.Config) (types.Engine, error) {
	return newEngine(cfg)
}
