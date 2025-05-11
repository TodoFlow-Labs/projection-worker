package handlers_test

import (
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"

	"github.com/todoflow-labs/projection-worker/internal/handlers"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type MockIndexer struct {
	mock.Mock
}

func (m *MockIndexer) Create(id string, doc any) {
	m.Called(id, doc)
}

func (m *MockIndexer) Update(id string, doc any) {
	m.Called(id, doc)
}

func (m *MockIndexer) Delete(id string) {
	m.Called(id)
}

func TestHandler_HandleCreate(t *testing.T) {
	mockIdx := new(MockIndexer)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(mockIdx, logger)

	event := dto.TodoCreatedEvent{
		BaseEvent: dto.BaseEvent{
			ID:   "c1",
			Type: dto.TodoCreatedEvt,
		},
		Title: "test",
	}
	data, _ := json.Marshal(event)
	mockIdx.On("Create", "c1", event).Once()

	msg := &nats.Msg{Data: data}
	handler.Handle(msg)

	mockIdx.AssertExpectations(t)
}

func TestHandler_HandleUpdate(t *testing.T) {
	mockIdx := new(MockIndexer)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(mockIdx, logger)

	event := dto.TodoUpdatedEvent{
		BaseEvent: dto.BaseEvent{
			ID:   "u1",
			Type: dto.TodoUpdatedEvt,
		},
		Title: "test",
	}
	data, _ := json.Marshal(event)
	mockIdx.On("Update", "u1", event).Once()

	msg := &nats.Msg{Data: data}
	handler.Handle(msg)

	mockIdx.AssertExpectations(t)
}

func TestHandler_HandleDelete(t *testing.T) {
	mockIdx := new(MockIndexer)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(mockIdx, logger)

	event := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			ID:   "d1",
			Type: dto.TodoDeletedEvt,
		},
	}
	data, _ := json.Marshal(event)
	mockIdx.On("Delete", "d1").Once()

	msg := &nats.Msg{Data: data}
	handler.Handle(msg)

	mockIdx.AssertExpectations(t)
}

func TestHandler_HandleUnknownEvent(t *testing.T) {
	mockIdx := new(MockIndexer)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(mockIdx, logger)

	base := dto.BaseEvent{ID: "x1", Type: "SomethingUnknown"}
	data, _ := json.Marshal(base)

	msg := &nats.Msg{Data: data}
	handler.Handle(msg)

	// nothing should be called
	mockIdx.AssertExpectations(t)
}

func TestHandler_HandleInvalidJSON(t *testing.T) {
	mockIdx := new(MockIndexer)
	logger := logging.New("debug")
	handler := handlers.NewTodoHandler(mockIdx, logger)

	msg := &nats.Msg{Data: []byte("not valid json")}
	handler.Handle(msg)

	// nothing should be called
	mockIdx.AssertExpectations(t)
}
