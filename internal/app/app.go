package app

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/todoflow-labs/projection-worker/internal/config"
	"github.com/todoflow-labs/projection-worker/internal/handlers"
	"github.com/todoflow-labs/projection-worker/internal/nats"
	"github.com/todoflow-labs/projection-worker/internal/storage"
	"github.com/todoflow-labs/shared-dtos/logging"
	"github.com/todoflow-labs/shared-dtos/metrics"
)

func Run() {
	cfg, err := config.Load()
	if err != nil {
		panic("config.Load failed: " + err.Error())
	}

	logger := logging.New(cfg.LogLevel).With().Str("service", "projection-worker").Logger()
	logger.Info().Msg("projection-worker starting")
	logger.Debug().Interface("config", cfg).Msg("loaded config")

	metrics.Init(cfg.MetricsAddr)
	logger.Debug().Msgf("metrics server started at %s", cfg.MetricsAddr)

	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to PostgreSQL")
	}
	defer db.Close()

	repo := storage.NewRepository(pgxExecutor{db}, logger)
	handler := handlers.NewTodoHandler(repo, logger)

	consumer := nats.NewConsumer(cfg, logger)
	if err := consumer.Consume(handler); err != nil {
		logger.Fatal().Err(err).Msg("consumer failed")
	}
}

type pgxExecutor struct {
	*pgxpool.Pool
}

func (p pgxExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.Pool.Exec(ctx, sql, args...)
}
