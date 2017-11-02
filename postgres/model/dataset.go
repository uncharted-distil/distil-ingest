package model

import (
	"github.com/unchartedsoftware/distil-ingest/metadata"
)

// Dataset is a struct containing the metadata of a dataset being processed.
type Dataset struct {
	ID              string
	Name            string
	Description     string
	Variables       []*metadata.Variable
	variablesLookup map[string]bool
}

// NewDataset creates a new dataset instance.
func NewDataset(id, name, description string, meta *metadata.Metadata) *Dataset {
	ds := &Dataset{
		ID:              id,
		Name:            name,
		Description:     description,
		variablesLookup: make(map[string]bool),
	}
	if meta != nil {
		ds.Variables = meta.Variables
	}

	return ds
}

// HasVariable checks to see if a variable is already contained in the dataset.
func (ds *Dataset) HasVariable(variable *metadata.Variable) bool {
	return ds.variablesLookup[variable.Name]
}

// AddVariable adds a variable to the dataset.
func (ds *Dataset) AddVariable(variable *metadata.Variable) {
	ds.Variables = append(ds.Variables, variable)
	ds.variablesLookup[variable.Name] = true
}
