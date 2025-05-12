package handlers

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type TodoEventHandler interface {
	Handle(m *nats.Msg) error
}

type Repository interface {
	Create(ctx context.Context, doc dto.SearchResult) error
	Update(ctx context.Context, doc dto.SearchResult) error
	Delete(ctx context.Context, id string) error
}

type Handler struct {
	repo   Repository
	logger logging.Logger
}

func NewTodoHandler(repo Repository, logger logging.Logger) TodoEventHandler {
	return &Handler{repo: repo, logger: logger}
}

func (h *Handler) Handle(m *nats.Msg) error {
	var base dto.BaseEvent
	if err := json.Unmarshal(m.Data, &base); err != nil {
		h.logger.Error().Err(err).Msg("unmarshal BaseEvent failed")
		return err
	}

	h.logger.Debug().Msgf("received event: %s (id=%s)", base.Type, base.ID)

	switch base.Type {
	case dto.TodoCreatedEvt:
		var ev dto.TodoCreatedEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			h.logger.Error().Err(err).Msg("unmarshal TodoCreatedEvent failed")
			return err
		}
		h.logger.Debug().Msgf("handling TodoCreatedEvent id=%s", ev.ID)
		return h.repo.Create(context.Background(), dto.SearchResult{
			ID:          ev.ID,
			UserID:      ev.UserID,
			Title:       ev.Title,
			Description: ev.Description,
			Completed:   false,
			DueDate:     ev.DueDate,
			Priority:    ev.Priority,
			Tags:        ev.Tags,
			CreatedAt:   ev.Timestamp,
			UpdatedAt:   ev.Timestamp,
		})

	case dto.TodoUpdatedEvt:
		var ev dto.TodoUpdatedEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			h.logger.Error().Err(err).Msg("unmarshal TodoUpdatedEvent failed")
			return err
		}
		h.logger.Debug().Msgf("handling TodoUpdatedEvent id=%s", ev.ID)
		return h.repo.Update(context.Background(), dto.SearchResult{
			ID:          ev.ID,
			UserID:      ev.UserID,
			Title:       ev.Title,
			Description: ev.Description,
			Completed:   ev.Completed,
			DueDate:     ev.DueDate,
			Priority:    ev.Priority,
			Tags:        ev.Tags,
			UpdatedAt:   ev.Timestamp,
		})

	case dto.TodoDeletedEvt:
		var ev dto.TodoDeletedEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			h.logger.Error().Err(err).Msg("unmarshal TodoDeletedEvent failed")
			return err
		}
		h.logger.Debug().Msgf("handling TodoDeletedEvent id=%s", ev.ID)
		return h.repo.Delete(context.Background(), ev.ID)

	default:
		h.logger.Warn().Msgf("unknown event type: %s", base.Type)
		return nil // Don't treat unknown as error, or optionally return fmt.Errorf("unsupported type")
	}
}
