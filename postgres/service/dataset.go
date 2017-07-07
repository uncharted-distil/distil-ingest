package service

import (
	"github.com/go-pg/pg"

	"github.com/unchartedsoftware/distil-ingest/postgres/model"
)

type DatasetService struct {
	DB *pg.DB
}

func NewDatasetService(db *pg.DB) *DatasetService {

	return &DatasetService{
		DB: db,
	}
}

func (s *DatasetService) GetByID(id string) (*model.Dataset, error) {
	var u model.Dataset
	_, err := s.DB.QueryOne(&u, `
        SELECT * FROM dataset
        WHERE id = ?
    `, id)
	return &u, err
}

func (s *DatasetService) Insert(dataset *model.Dataset) error {
	return s.DB.Insert(dataset)
}
