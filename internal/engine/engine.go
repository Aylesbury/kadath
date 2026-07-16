package engine

import (
	"fmt"
	"strings"

	"starless/kadath/configs"
	"starless/kadath/internal/types"
)

type builderFunc func(cfg *configs.Config) (types.Engine, error)

// builders is populated by init() in the engine_*.go files compiled in via
// build tags. Single-engine builds (-tags postgres) register one entry; the
// "all" tag compiles every engine into one binary for platform runners.
var builders = map[string]builderFunc{}

func register(name string, b builderFunc) {
	builders[name] = b
}

// aliases map engine names to the driver that speaks their wire protocol.
var aliases = map[string]string{
	"postgres": "postgresql",
	"redshift": "postgresql",
}

// NewEngine picks the engine for the given connection at runtime: the
// explicit type (per-job connection block or DB_TYPE) wins, falling back to
// the DSN scheme.
func NewEngine(cfg *configs.Config) (types.Engine, error) {
	name := cfg.EngineType
	if name == "" {
		name = typeFromDSN(cfg.DSN)
	}
	if alias, ok := aliases[name]; ok {
		name = alias
	}
	if name == "" {
		return nil, fmt.Errorf("cannot determine database engine: set DB_TYPE or use a DSN with a scheme")
	}

	builder, ok := builders[name]
	if !ok {
		return nil, fmt.Errorf("engine %q is not built into this binary (available: %s)", name, availableEngines())
	}

	return builder(cfg)
}

func typeFromDSN(dsn string) string {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return "postgresql"
	case strings.HasPrefix(dsn, "mysql://"):
		return "mysql"
	case strings.HasPrefix(dsn, "mssql://"), strings.HasPrefix(dsn, "sqlserver://"):
		return "mssql"
	}
	return ""
}

func availableEngines() string {
	if len(builders) == 0 {
		return "none"
	}
	names := make([]string, 0, len(builders))
	for name := range builders {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}
