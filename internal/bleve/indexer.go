package bleve

import (
	"os"

	"github.com/blevesearch/bleve/v2"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type Indexer struct {
	path   string
	logger *logging.Logger
}

func NewIndexer(path string, logger *logging.Logger) *Indexer {
	return &Indexer{path, logger}
}

func (i *Indexer) openIndex() (bleve.Index, error) {
	i.logger.Debug().Msgf("opening index at path: %s", i.path)
	if _, err := os.Stat(i.path); os.IsNotExist(err) {
		i.logger.Debug().Msg("index does not exist, creating new")
		mapping := bleve.NewIndexMapping()
		return bleve.New(i.path, mapping)
	}
	return bleve.Open(i.path)
}

func (i *Indexer) Create(id string, doc interface{}) {
	index, err := i.openIndex()
	if err != nil {
		i.logger.Error().Err(err).Msg("bleve.Open failed")
		return
	}
	defer index.Close()
	i.logger.Debug().Msgf("indexing (create) document with id: %s", id)
	index.Index(id, doc)
}

func (i *Indexer) Update(id string, doc interface{}) {
	i.logger.Debug().Msgf("indexing (update) document with id: %s", id)
	i.Create(id, doc)
}

func (i *Indexer) Delete(id string) {
	index, err := i.openIndex()
	if err != nil {
		i.logger.Error().Err(err).Msg("bleve.Open failed")
		return
	}
	defer index.Close()
	i.logger.Debug().Msgf("deleting document with id: %s", id)
	index.Delete(id)
}
