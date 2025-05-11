package app

import (
	"github.com/todoflow-labs/projection-worker/internal/bleve"
	"github.com/todoflow-labs/projection-worker/internal/config"
	"github.com/todoflow-labs/projection-worker/internal/handlers"
	"github.com/todoflow-labs/projection-worker/internal/nats"
	"github.com/todoflow-labs/shared-dtos/logging"
	"github.com/todoflow-labs/shared-dtos/metrics"
)

func Run() {
	cfg, err := config.Load()
	if err != nil {
		panic("config.Load failed: " + err.Error())
	}

	logger := logging.New(cfg.LogLevel)
	logger.Info().Msg("projection-worker starting")
	logger.Debug().Interface("config", cfg).Msg("loaded config")

	metrics.Init(cfg.MetricsAddr)
	logger.Debug().Msgf("metrics server started at %s", cfg.MetricsAddr)

	consumer := nats.NewConsumer(cfg, logger)
	indexer := bleve.NewIndexer(cfg.BleveIndexPath, logger)
	handler := handlers.NewTodoHandler(indexer, logger)

	if err := consumer.Consume(handler); err != nil {
		logger.Fatal().Err(err).Msg("consumer failed")
	}
}
