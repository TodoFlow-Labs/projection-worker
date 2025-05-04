package config

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	// URL of your NATS+JetStream server
	NATSURL string
	// Filesystem path to your Bleve index
	BleveIndexPath string
	// Logging level: debug, info, warn, error
	LogLevel string
}

func Load() (*Config, error) {
	// 1) Define flags
	pflag.String("nats-url", "nats://localhost:4222", "NATS JetStream server URL")
	pflag.String("bleve-index-path", "./index.bleve", "Path to Bleve index directory")
	pflag.String("log-level", "info", "Log verbosity (debug|info|warn|error)")
	pflag.Parse()

	// 2) Bind flags to viper
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return nil, err
	}

	// 3) Allow env vars like PROJECTION_WORKER_NATS_URL or BLEVE_INDEX_PATH
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// 4) Construct the config
	cfg := &Config{
		NATSURL:        viper.GetString("nats-url"),
		BleveIndexPath: viper.GetString("bleve-index-path"),
		LogLevel:       viper.GetString("log-level"),
	}

	// 5) Validate required fields
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("nats-url must be set")
	}
	if cfg.BleveIndexPath == "" {
		return nil, fmt.Errorf("bleve-index-path must be set")
	}

	return cfg, nil
}
