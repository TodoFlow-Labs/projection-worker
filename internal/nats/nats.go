package nats

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/projection-worker/internal/config"
	"github.com/todoflow-labs/projection-worker/internal/handlers"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type Consumer struct {
	js     nats.JetStreamContext
	sub    *nats.Subscription
	logger *logging.Logger
}

func NewConsumer(cfg *config.Config, logger *logging.Logger) *Consumer {
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats.Connect failed")
	}
	logger.Debug().Msg("connected to NATS")

	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("JetStream init failed")
	}
	logger.Debug().Msg("JetStream context initialized")

	sub, err := js.PullSubscribe("todo.events", "projection-worker",
		nats.PullMaxWaiting(128),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("PullSubscribe failed")
	}
	logger.Info().Msg("subscribed to todo.events (pull mode)")

	return &Consumer{js, sub, logger}
}

func (c *Consumer) Consume(handler handlers.TodoEventHandler) error {
	c.logger.Debug().Msg("starting message consume loop")
	for {
		batch, err := c.sub.Fetch(5, nats.MaxWait(2*time.Second))
		if err != nil && err != nats.ErrTimeout {
			c.logger.Error().Err(err).Msg("Fetch error")
			continue
		}

		if len(batch) == 0 {
			c.logger.Debug().Msg("no messages fetched, sleeping")
		} else {
			c.logger.Debug().Msgf("fetched %d message(s)", len(batch))
		}

		for _, m := range batch {
			c.logger.Debug().Msg("processing message")
			handler.Handle(m)
			c.logger.Debug().Msg("message acknowledged")
			m.Ack()
		}
	}
}
