package storage

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type PGExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type Repository struct {
	db     PGExecutor
	logger logging.Logger
}

func NewRepository(db PGExecutor, logger logging.Logger) *Repository {
	return &Repository{db: db, logger: logger}
}

type RepositoryInterface interface {
	Create(ctx context.Context, doc dto.SearchResult) error
	Update(ctx context.Context, doc dto.SearchResult) error
	Delete(ctx context.Context, id string) error
}

func (r *Repository) Create(ctx context.Context, doc dto.SearchResult) error {
	r.logger.Debug().Str("id", doc.ID).Msg("inserting todo")

	_, err := r.db.Exec(ctx, `
		INSERT INTO todo (
			id, user_id, title, description, completed,
			due_date, priority, tags, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5,
		        $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING
	`, doc.ID, doc.UserID, doc.Title, doc.Description, doc.Completed,
		doc.DueDate, doc.Priority, doc.Tags, doc.CreatedAt, doc.UpdatedAt)

	return err
}

func (r *Repository) Update(ctx context.Context, doc dto.SearchResult) error {
	r.logger.Debug().Str("id", doc.ID).Msg("updating todo")

	_, err := r.db.Exec(ctx, `
		UPDATE todo SET
			title = $2,
			description = $3,
			completed = $4,
			due_date = $5,
			priority = $6,
			tags = $7,
			updated_at = $8
		WHERE id = $1
	`, doc.ID, doc.Title, doc.Description, doc.Completed,
		doc.DueDate, doc.Priority, doc.Tags, doc.UpdatedAt)

	return err
}

func (r *Repository) Delete(ctx context.Context, id string) error {
	r.logger.Debug().Str("id", id).Msg("deleting todo")
	_, err := r.db.Exec(ctx, `DELETE FROM todo WHERE id = $1`, id)
	return err
}
