package configs

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {

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
