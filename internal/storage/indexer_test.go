package storage_test

import (
	"context"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"

	"github.com/todoflow-labs/projection-worker/internal/storage"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

func sampleTodo() dto.SearchResult {
	return dto.SearchResult{
		ID:          "todo-id",
		UserID:      "user-id",
		Title:       "Test Todo",
		Description: "Something important",
		Completed:   true,
		CreatedAt:   time.Now().Truncate(time.Millisecond),
		UpdatedAt:   time.Now().Truncate(time.Millisecond),
		DueDate:     nil,
		Priority:    nil,
		Tags:        []string{"urgent", "personal"},
	}
}

func TestRepository_Create(t *testing.T) {
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mock.Close()

	logger := logging.New("debug")
	repo := storage.NewRepository(mock, logger)
	todo := sampleTodo()

	mock.ExpectExec("INSERT INTO todo").
		WithArgs(todo.ID, todo.UserID, todo.Title, todo.Description, todo.Completed,
			todo.DueDate, todo.Priority, todo.Tags, todo.CreatedAt, todo.UpdatedAt).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = repo.Create(context.Background(), todo)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Update(t *testing.T) {
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mock.Close()

	logger := logging.New("debug")
	repo := storage.NewRepository(mock, logger)
	todo := sampleTodo()

	mock.ExpectExec("UPDATE todo").
		WithArgs(todo.ID, todo.Title, todo.Description, todo.Completed,
			todo.DueDate, todo.Priority, todo.Tags, todo.UpdatedAt).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = repo.Update(context.Background(), todo)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Delete(t *testing.T) {
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mock.Close()

	logger := logging.New("debug")
	repo := storage.NewRepository(mock, logger)

	mock.ExpectExec("DELETE FROM todo").
		WithArgs("todo-id").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))

	err = repo.Delete(context.Background(), "todo-id")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
