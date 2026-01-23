//go:build postgres

package engine

import (
	"starless/kadath/configs"
	"starless/kadath/internal/engine/postgres"
	"starless/kadath/internal/types"
)

func newEngine(cfg *configs.Config) (types.Engine, error) {
	return postgres.NewEngine(cfg)
}
