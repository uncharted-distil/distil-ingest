package d3mdata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"

	"github.com/unchartedsoftware/distil-ingest/metadata"
)

// D3MData is a row from a CSV file
type D3MData struct {
	document.CSV
	meta  *metadata.Metadata
	idCol int
}

func getIDColumn(meta *metadata.Metadata) (int, error) {
	for index, v := range meta.Variables {
		if v.Name == "d3mIndex" {
			return index, nil
		}
	}
	return -1, errors.Errorf("no id column found")
}

// NewD3MData instantiates and returns a new document using metadata.
func NewD3MData(meta *metadata.Metadata) (deluge.Constructor, error) {
	// get id column and cache for later
	idCol, err := getIDColumn(meta)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse schema file")
	}
	return func() (deluge.Document, error) {
		return &D3MData{
			meta:  meta,
			idCol: idCol,
		}, nil
	}, nil
}

// GetID returns the document id.
func (d *D3MData) GetID() (string, error) {
	return d.Cols[d.idCol], nil
}

// GetType returns the document type.
func (d *D3MData) GetType() (string, error) {
	return "datum", nil
}

// GetMapping returns the document's mappings.
func (d *D3MData) GetMapping() (string, error) {
	// create the ES mappings based on the variables in the schema
	mappings := gabs.New()

	for _, v := range d.meta.Variables {

		if v.SelectedRole != "attribute" && v.SelectedRole != "target" {
			continue
		}

		var varType string

		switch v.Type {
		case "integer", "index":
			varType = "long"
			break
		case "float", "latitude", "longitude":
			varType = "double"
			break
		case "text":
			varType = "text"
			break
		case "categorical", "ordinal", "address", "city", "state", "country", "email", "phone", "postal_code", "uri":
			varType = "keyword"
			break
		case "dateTime":
			varType = "date"
			break
		case "boolean":
			varType = "boolean"
			break
		default:
			return "", fmt.Errorf("Unknown data type %s", varType)
		}

		varNameKey := fmt.Sprintf("datum.properties.%s.properties.value.type", v.Name)
		varTypeKey := fmt.Sprintf("datum.properties.%s.properties.schemaType.type", v.Name)

		mappings.SetP(varType, varNameKey)
		mappings.SetP("keyword", varTypeKey)
	}

	return mappings.String(), nil
}

// GetSource returns the source document in JSON format
func (d *D3MData) GetSource() (interface{}, error) {
	source := make(map[string]interface{})

	for index, v := range d.meta.Variables {
		if v.SelectedRole != "attribute" && v.SelectedRole != "target" {
			continue
		}

		var varValue interface{}

		switch v.Type {
		case "integer", "index", "dateTime":
			varValue, _ = d.Int64(index)
			break
		case "float", "latitude", "longitude":
			varValue, _ = d.Float64(index)
			break
		case "text", "categorical", "ordinal", "address", "city", "state", "country", "email", "phone", "postal_code", "uri":
			varValue, _ = d.String(index)
			break
		case "boolean":
			varValue, _ = d.Bool(index)
			break
		default:
			return "", fmt.Errorf("unknown data type %s", v.Type)
		}

		// set entry
		source[v.Name] = map[string]interface{}{
			"value":      varValue,
			"schemaType": v.Type,
		}
	}

	return source, nil
}
