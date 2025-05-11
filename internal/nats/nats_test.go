package nats_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/todoflow-labs/projection-worker/internal/config"
	my_nats "github.com/todoflow-labs/projection-worker/internal/nats"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type MockHandler struct {
	mock.Mock
}

func (m *MockHandler) Handle(msg *nats.Msg) {
	m.Called(msg)
}

func startEmbeddedNATSServer(t *testing.T) (*server.Server, string) {
	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	return srv, srv.ClientURL()
}

func TestConsumer_ReceivesAndHandlesMessage(t *testing.T) {
	srv, natsURL := startEmbeddedNATSServer(t)
	defer srv.Shutdown()

	cfg := &config.Config{NATSURL: natsURL}
	logger := logging.New("debug")

	// Connect and set up JetStream + stream
	nc, _ := nats.Connect(natsURL)
	js, _ := nc.JetStream()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "todo_events",
		Subjects: []string{"todo.events"},
	})
	require.NoError(t, err)

	// Publish a test message
	js.Publish("todo.events", []byte(`{"id":"abc","type":"TodoCreatedEvent"}`))

	mockHandler := new(MockHandler)
	mockHandler.On("Handle", mock.Anything).Once()

	my_consumer := my_nats.NewConsumer(cfg, logger)

	// Run consume in background
	go func() {
		err := my_consumer.Consume(mockHandler)
		require.NoError(t, err)
	}()

	// Give it some time to receive and process
	time.Sleep(1 * time.Second)

	mockHandler.AssertExpectations(t)
}
