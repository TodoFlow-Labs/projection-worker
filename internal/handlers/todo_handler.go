package handlers

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/projection-worker/internal/bleve"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type TodoEventHandler interface {
	Handle(m *nats.Msg)
}

type Handler struct {
	indexer bleve.IndexerInterface
	logger  *logging.Logger
}

func NewTodoHandler(indexer bleve.IndexerInterface, logger *logging.Logger) TodoEventHandler {
	return &Handler{indexer, logger}
}

func (h *Handler) Handle(m *nats.Msg) {
	var base dto.BaseEvent
	if err := json.Unmarshal(m.Data, &base); err != nil {
		h.logger.Error().Err(err).Msg("unmarshal BaseEvent failed")
		return
	}

	h.logger.Debug().Msgf("received event: %s (id=%s)", base.Type, base.ID)

	switch base.Type {
	case dto.TodoCreatedEvt:
		var ev dto.TodoCreatedEvent
		if json.Unmarshal(m.Data, &ev) == nil {
			h.logger.Debug().Msgf("handling TodoCreatedEvent id=%s", ev.ID)
			h.indexer.Create(ev.ID, ev)
		}
	case dto.TodoUpdatedEvt:
		var ev dto.TodoUpdatedEvent
		if json.Unmarshal(m.Data, &ev) == nil {
			h.logger.Debug().Msgf("handling TodoUpdatedEvent id=%s", ev.ID)
			h.indexer.Update(ev.ID, ev)
		}
	case dto.TodoDeletedEvt:
		var ev dto.TodoDeletedEvent
		if json.Unmarshal(m.Data, &ev) == nil {
			h.logger.Debug().Msgf("handling TodoDeletedEvent id=%s", ev.ID)
			h.indexer.Delete(ev.ID)
		}
	default:
		h.logger.Warn().Msgf("unknown event type: %s", base.Type)
	}
}
