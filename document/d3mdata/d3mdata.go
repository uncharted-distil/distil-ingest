package d3mdata

import (
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"
)

// D3MData is a row from a CSV file
type D3MData struct {
	document.CSV
}

// NewD3MData instantiates and returns a new document.
func NewD3MData() (deluge.Document, error) {
	return &D3MData{}, nil
}

// GetID returns the document id.
func (d *D3MData) GetID() (string, error) {
	return "id", nil
}

// GetType returns the document type.
func (d *D3MData) GetType() (string, error) {
	return "d3m-csv", nil
}

// GetMapping returns the documents mappings.
func (d *D3MData) GetMapping() (string, error) {
	// get mapping
	return string(""), nil
}

// GetSource returns the marshalled source portion of the document in
// a tagged structure.
func (d *D3MData) GetSource() (interface{}, error) {
	// pull the columns out of the schema
	return nil, nil
}
