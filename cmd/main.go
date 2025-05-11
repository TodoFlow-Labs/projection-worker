package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/nats-io/nats.go"

	"github.com/todoflow-labs/projection-worker/internal/config"

	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
	"github.com/todoflow-labs/shared-dtos/metrics"
)

func main() {
	// 1) Load config
	cfg, err := config.Load()
	if err != nil {
		panic("config.Load failed: " + err.Error())
	}
	logger := logging.New(cfg.LogLevel)
	logger.Info().Msg("projection-worker starting")

	metrics.Init(cfg.MetricsAddr)
	logger.Debug().Msgf("metrics server listening on %s", cfg.MetricsAddr)

	// 2) Connect to NATS + JetStream once
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats.Connect failed")
	}
	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("JetStream init failed")
	}

	// 3) Set up a durable pull-subscription (we’ll Fetch each loop)
	sub, err := js.PullSubscribe(
		"todo.events",
		"projection-worker",
		nats.PullMaxWaiting(128),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("PullSubscribe failed")
	}
	logger.Info().Msg("subscribed to todo.events (pull mode)")

	// 4) Processing loop: fetch → open index → apply → close index → repeat
	for {
		batch, err := sub.Fetch(5, nats.MaxWait(2*time.Second))
		if err != nil && err != nats.ErrTimeout {
			logger.Error().Err(err).Msg("Fetch error")
			continue
		}
		if len(batch) == 0 {
			// nothing to do right now
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// 4a) Open (or create) the index for this batch
		logger.Debug().Msgf("opening Bleve index at %s", cfg.BleveIndexPath)
		var index bleve.Index
		if _, err := os.Stat(cfg.BleveIndexPath); os.IsNotExist(err) {
			mapping := bleve.NewIndexMapping()
			index, err = bleve.New(cfg.BleveIndexPath, mapping)
			if err != nil {
				logger.Fatal().Err(err).Msg("bleve.New failed")
			}
			logger.Info().Msgf("created new Bleve index at %s", cfg.BleveIndexPath)
		} else {
			index, err = bleve.Open(cfg.BleveIndexPath)
			if err != nil {
				logger.Fatal().Err(err).Msg("bleve.Open failed")
			}
			logger.Info().Msgf("opened existing Bleve index at %s", cfg.BleveIndexPath)
		}

		// 4b) Apply each event
		for _, m := range batch {
			var base dto.BaseEvent
			if err := json.Unmarshal(m.Data, &base); err != nil {
				logger.Error().Err(err).Msg("unmarshal BaseEvent failed")
				m.Ack()
				continue
			}

			logger.Debug().Msgf("event received: %s id=%s", base.Type, base.ID)

			switch base.Type {
			case dto.TodoCreatedEvt:
				var ev dto.TodoCreatedEvent
				if err := json.Unmarshal(m.Data, &ev); err != nil {
					logger.Error().Err(err).Msg("unmarshal TodoCreatedEvent failed")
					break
				}
				if err := index.Index(ev.ID, ev); err != nil {
					logger.Error().Err(err).Msg("bleve.Index (create) failed")
				} else {
					logger.Debug().Msgf("indexed created todo id=%s", ev.ID)
				}

			case dto.TodoUpdatedEvt:
				var ev dto.TodoUpdatedEvent
				if err := json.Unmarshal(m.Data, &ev); err != nil {
					logger.Error().Err(err).Msg("unmarshal TodoUpdatedEvent failed")
					break
				}
				if err := index.Index(ev.ID, ev); err != nil {
					logger.Error().Err(err).Msg("bleve.Index (update) failed")
				} else {
					logger.Debug().Msgf("indexed updated todo id=%s", ev.ID)
				}

			case dto.TodoDeletedEvt:
				var ev dto.TodoDeletedEvent
				if err := json.Unmarshal(m.Data, &ev); err != nil {
					logger.Error().Err(err).Msg("unmarshal TodoDeletedEvent failed")
					break
				}
				if err := index.Delete(ev.ID); err != nil {
					logger.Error().Err(err).Msg("bleve.Delete failed")
				} else {
					logger.Debug().Msgf("deleted todo id=%s from index", ev.ID)
				}

			default:
				logger.Warn().Msgf("unknown event type: %s", base.Type)
			}

			// 4c) Ack the message
			m.Ack()
		}

		// 4d) Close the index to release the file lock immediately
		if err := index.Close(); err != nil {
			logger.Error().Err(err).Msg("bleve.Close failed")
		} else {
			logger.Debug().Msg("closed Bleve index")
		}
	}
}
