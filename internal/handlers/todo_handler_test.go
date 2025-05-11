package handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/todoflow-labs/projection-worker/internal/handlers"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) Create(ctx context.Context, doc dto.SearchResult) error {
	args := m.Called(ctx, doc)
	return args.Error(0)
}

func (m *mockRepo) Update(ctx context.Context, doc dto.SearchResult) error {
	args := m.Called(ctx, doc)
	return args.Error(0)
}

func (m *mockRepo) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func newTestMsg(t *testing.T, v interface{}) *nats.Msg {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return &nats.Msg{Data: data}
}

func TestHandler_HandleCreateEvent(t *testing.T) {
	repo := new(mockRepo)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(repo, logger)

	now := time.Now().Truncate(time.Millisecond)
	ev := dto.TodoCreatedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoCreatedEvt,
			ID:        "c1",
			UserID:    "u1",
			Timestamp: now,
		},
		Title:       "test",
		Description: "desc",
		DueDate:     nil,
		Priority:    nil,
		Tags:        []string{"tag1", "tag2"},
	}

	repo.On("Create", mock.Anything, mock.MatchedBy(func(doc dto.SearchResult) bool {
		assert.Equal(t, ev.ID, doc.ID)
		assert.Equal(t, ev.UserID, doc.UserID)
		assert.Equal(t, ev.Title, doc.Title)
		assert.Equal(t, ev.Description, doc.Description)
		assert.Equal(t, ev.Tags, doc.Tags)
		assert.Equal(t, false, doc.Completed)
		assert.WithinDuration(t, now, doc.CreatedAt, time.Millisecond)
		assert.WithinDuration(t, now, doc.UpdatedAt, time.Millisecond)
		return true
	})).Return(nil).Once()

	handler.Handle(newTestMsg(t, ev))
	repo.AssertExpectations(t)
}

func TestHandler_HandleUpdateEvent(t *testing.T) {
	repo := new(mockRepo)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(repo, logger)

	now := time.Now().Truncate(time.Millisecond)
	ev := dto.TodoUpdatedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoUpdatedEvt,
			ID:        "c1",
			UserID:    "u1",
			Timestamp: now,
		},
		Title:       "updated",
		Description: "updated desc",
		Completed:   true,
		DueDate:     nil,
		Priority:    nil,
		Tags:        []string{"tagA"},
	}

	repo.On("Update", mock.Anything, mock.MatchedBy(func(doc dto.SearchResult) bool {
		assert.Equal(t, ev.ID, doc.ID)
		assert.Equal(t, ev.UserID, doc.UserID)
		assert.Equal(t, ev.Title, doc.Title)
		assert.Equal(t, ev.Description, doc.Description)
		assert.Equal(t, ev.Completed, doc.Completed)
		assert.Equal(t, ev.Tags, doc.Tags)
		assert.WithinDuration(t, now, doc.UpdatedAt, time.Millisecond)
		return true
	})).Return(nil).Once()

	handler.Handle(newTestMsg(t, ev))
	repo.AssertExpectations(t)
}

func TestHandler_HandleDeleteEvent(t *testing.T) {
	repo := new(mockRepo)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(repo, logger)

	ev := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			Type: dto.TodoDeletedEvt,
			ID:   "d1",
		},
	}

	repo.On("Delete", mock.Anything, "d1").Return(nil).Once()
	handler.Handle(newTestMsg(t, ev))
	repo.AssertExpectations(t)
}

func TestHandler_IgnoreUnknownEvent(t *testing.T) {
	repo := new(mockRepo)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(repo, logger)

	ev := dto.BaseEvent{
		Type: "UnknownEventType",
		ID:   "x1",
	}

	handler.Handle(newTestMsg(t, ev)) // should not panic or call repo
	repo.AssertExpectations(t)
}
