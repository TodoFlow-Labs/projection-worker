package bleve_test

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/stretchr/testify/assert"
	my_bleve "github.com/todoflow-labs/projection-worker/internal/bleve"
	"github.com/todoflow-labs/shared-dtos/logging"
)

func TestIndexer_CreateUpdateDelete(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := tempDir + "/test.bleve"
	logger := logging.New("debug")
	indexer := my_bleve.NewIndexer(indexPath, logger)

	docID := "todo123"
	doc := map[string]string{"title": "Test Todo"}

	// Create
	t.Log("Creating...")
	indexer.Create(docID, doc)

	t.Log("Verifying create...")
	func() {
		idx, err := bleve.Open(indexPath)
		assert.NoError(t, err)
		defer idx.Close()

		docResult, err := idx.Document(docID)
		assert.NoError(t, err)
		assert.NotNil(t, docResult)
	}()

	// Update
	t.Log("Updating...")
	doc["title"] = "Updated Todo"
	indexer.Update(docID, doc)

	t.Log("Verifying update...")
	func() {
		idx, err := bleve.Open(indexPath)
		assert.NoError(t, err)
		defer idx.Close()

		docResult, err := idx.Document(docID)
		assert.NoError(t, err)
		assert.NotNil(t, docResult)
	}()

	// Delete
	t.Log("Deleting...")
	indexer.Delete(docID)

	t.Log("Verifying delete...")
	func() {
		idx, err := bleve.Open(indexPath)
		assert.NoError(t, err)
		defer idx.Close()

		docResult, _ := idx.Document(docID)
		assert.Nil(t, docResult)
	}()
}
