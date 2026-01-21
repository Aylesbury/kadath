package configs

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	ConnectorId	string	`envconfig:"CONNECTOR_ID"`
	AuthToken		string	`envconfig:"AUTH_TOKEN"`
	DSN					string	`envconfig:"DB_URL"`
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

	return cfg, nil
}
