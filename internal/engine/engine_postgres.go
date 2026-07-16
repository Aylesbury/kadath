//go:build postgres || all

package engine

import (
	"starless/kadath/internal/engine/postgres"
)

func init() {
	register("postgresql", postgres.NewEngine)
}
