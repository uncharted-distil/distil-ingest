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
	insertBatch     []string
	insertArgs      []interface{}
}

// NewDataset creates a new dataset instance.
func NewDataset(id, name, description string, meta *metadata.Metadata) *Dataset {
	ds := &Dataset{
		ID:              id,
		Name:            name,
		Description:     description,
		variablesLookup: make(map[string]bool),
	}
	// NOTE: Can only support data in a single data resource for now.
	if meta != nil {
		ds.Variables = meta.DataResources[0].Variables
	}

	ds.ResetBatch()

	return ds
}

// ResetBatch clears the batch contents.
func (ds *Dataset) ResetBatch() {
	ds.insertBatch = make([]string, 0)
	ds.insertArgs = make([]interface{}, 0)
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

// AddInsert adds an insert statement and parameters to the batch.
func (ds *Dataset) AddInsert(statement string, args []interface{}) {
	ds.insertBatch = append(ds.insertBatch, statement)
	ds.insertArgs = append(ds.insertArgs, args...)
}

// GetBatch returns the insert statement batch.
func (ds *Dataset) GetBatch() []string {
	return ds.insertBatch
}

// GetBatchSize gets the insert batch count.
func (ds *Dataset) GetBatchSize() int {
	return len(ds.insertBatch)
}

// GetBatchArgs returns the insert batch arguments.
func (ds *Dataset) GetBatchArgs() []interface{} {
	return ds.insertArgs
}
