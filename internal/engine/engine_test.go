//go:build all

package engine

import (
	"testing"

	"starless/kadath/configs"
)

func TestTypeFromDSN(t *testing.T) {
	for dsn, want := range map[string]string{
		"postgres://u:p@h:5432/db":   "postgresql",
		"postgresql://u:p@h:5432/db": "postgresql",
		"mysql://u:p@h:3306/db":      "mysql",
		"sqlserver://u:p@h:1433/db":  "mssql",
		"bogus":                      "",
	} {
		if got := typeFromDSN(dsn); got != want {
			t.Errorf("typeFromDSN(%q) = %q, want %q", dsn, got, want)
		}
	}
}

func TestNewEngineDispatch(t *testing.T) {
	// redshift aliases to the postgres driver
	if _, err := NewEngine(&configs.Config{EngineType: "redshift", DSN: "postgresql://u:p@h:5432/db"}); err != nil {
		t.Errorf("redshift should dispatch to the postgres engine, got error: %v", err)
	}

	if _, err := NewEngine(&configs.Config{EngineType: "mysql", DSN: "mysql://u:p@h:3306/db"}); err != nil {
		t.Errorf("mysql should be built in with the all tag, got error: %v", err)
	}

	// engines that are not compiled in fail with a clear error
	if _, err := NewEngine(&configs.Config{EngineType: "mssql", DSN: "mssql://u:p@h:1433/db"}); err == nil {
		t.Error("mssql is not implemented and should error")
	}

	// no type and no scheme
	if _, err := NewEngine(&configs.Config{DSN: "not-a-dsn"}); err == nil {
		t.Error("undeterminable engine should error")
	}
}
