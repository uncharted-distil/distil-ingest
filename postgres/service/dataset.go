package service

import (
	"github.com/go-pg/pg"

	"github.com/unchartedsoftware/distil-ingest/postgres/model"
)

// DatasetService represents a dataset service.
type DatasetService struct {
	DB *pg.DB
}

// NewDatasetService instantiates and returns a dataset service.
func NewDatasetService(db *pg.DB) *DatasetService {

	return &DatasetService{
		DB: db,
	}
}

// GetByID returns a dataset byte id.
func (s *DatasetService) GetByID(id string) (*model.Dataset, error) {
	var u model.Dataset
	_, err := s.DB.QueryOne(&u, `
		SELECT * FROM dataset
		WHERE id = ?
	`, id)
	return &u, err
}

// Insert inserts a dataset.
func (s *DatasetService) Insert(dataset *model.Dataset) error {
	return s.DB.Insert(dataset)
}
