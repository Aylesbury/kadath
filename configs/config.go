package configs

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

const (
	// ModeConnector runs the agent for a single connector: the database
	// connection is injected locally via DB_URL and the auth token is
	// connector-scoped.
	ModeConnector = "connector"
	// ModeAdmin runs the agent as a privileged platform runner: it serves
	// every credentials-mode connector of its engine, authenticating with a
	// platform-scoped token. Connections arrive per job from the broker, so
	// no DB_URL is configured.
	ModeAdmin = "admin"
)

type Config struct {
	Mode        string `envconfig:"AGENT_MODE" default:"connector"`
	ServerAddr  string `envconfig:"SERVER_ADDR" default:"localhost:9001"`
	ServerTLS   bool   `envconfig:"SERVER_TLS" default:"false"`
	ConnectorId string `envconfig:"CONNECTOR_ID"`
	AuthToken   string `envconfig:"AUTH_TOKEN"`
	DSN         string `envconfig:"DB_URL"`
	SSLMode     string `envconfig:"DB_SSLMODE" default:"disable"`
	// EngineType selects the database engine ("postgresql", "mysql", ...).
	// Optional: inferred from the DSN scheme when empty. In admin mode it is
	// overridden per job by the connection block's type.
	EngineType string `envconfig:"DB_TYPE"`
}

// IsAdmin reports whether the agent runs as a platform runner.
// "platform" is accepted as an alias since that's the token scope name.
func (c *Config) IsAdmin() bool {
	return c.Mode == ModeAdmin || c.Mode == "platform"
}

func (c *Config) validate() error {
	if c.AuthToken == "" {
		return fmt.Errorf("AUTH_TOKEN is required")
	}

	switch {
	case c.IsAdmin():
		if c.DSN != "" {
			return fmt.Errorf("DB_URL must not be set in admin mode: connections arrive per job from the broker")
		}
	case c.Mode == ModeConnector:
		if c.DSN == "" {
			return fmt.Errorf("DB_URL is required in connector mode")
		}
	default:
		return fmt.Errorf("unknown AGENT_MODE %q (expected %q or %q)", c.Mode, ModeConnector, ModeAdmin)
	}

	return nil
}

func readEnv() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	return &cfg, err
}

func LoadConfig() (*Config, error) {
	cfg, err := readEnv()
	if err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}
