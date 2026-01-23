//go:build mysql

package engine

import (
	"starless/kadath/configs"
	"starless/kadath/internal/engine/mysql"
	"starless/kadath/internal/types"
)

func newEngine(cfg *configs.Config) (types.Engine, error) {
	return mysql.NewEngine(cfg)
}
