//go:build mysql || all

package engine

import (
	"starless/kadath/internal/engine/mysql"
)

func init() {
	register("mysql", mysql.NewEngine)
}
