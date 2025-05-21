package config

import (
	"fmt"
	"os"
)

type Config struct {
	NATSURL     string
	LogLevel    string
	MetricsAddr string
	DatabaseURL string
}

func Load() (*Config, error) {
	cfg := &Config{
		NATSURL:     os.Getenv("NATS_URL"),
		LogLevel:    os.Getenv("LOG_LEVEL"),
		MetricsAddr: os.Getenv("METRICS_ADDR"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}

	// Validation
	var missing []string
	if cfg.NATSURL == "" {
		missing = append(missing, "NATS_URL")
	}
	if cfg.LogLevel == "" {
		missing = append(missing, "LOG_LEVEL")
	}
	if cfg.MetricsAddr == "" {
		missing = append(missing, "METRICS_ADDR")
	}
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env vars: %v", missing)
	}

	return cfg, nil
}
