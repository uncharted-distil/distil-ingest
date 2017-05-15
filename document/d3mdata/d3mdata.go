package d3mdata

import (
	"io/ioutil"

	"fmt"

	"github.com/jeffail/gabs"
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"
	log "github.com/unchartedsoftware/plog"
)

// D3MData is a row from a CSV file
type D3MData struct {
	document.CSV
	schema *gabs.Container
}

// NewD3MData instantiates and returns a new document.
func NewD3MData(schemaPath string) deluge.Constructor {
	// Open the schema file
	dat, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		log.Error(err)
	}

	// Unmarshall the schema file
	schema, err := gabs.ParseJSON(dat)
	if err != nil {
		log.Error(err)
	}

	return func() (deluge.Document, error) {
		return &D3MData{schema: schema}, nil
	}
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

	return string(""), nil
}

// GetSource returns the source document in JSON format
func (d *D3MData) GetSource() (interface{}, error) {
	// grab the variable description portion of the schema

	trainingArray, err := d.schema.Path("trainData.trainData").Children()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// json container for extracted data
	varEntry := gabs.New()

	// iterate over the variable descriptions
	for index, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})

		// ignore anything other than attributes
		if varDesc["varRole"] == "attribute" {
			// grab name, type
			varName := varDesc["varName"].(string)
			varType := varDesc["varType"].(string)

			// var value will be based on the varType contents
			switch varType {
			case "integer":
				val, success := d.Int64(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "float":
				val, success := d.Float64(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "text":
				val, success := d.String(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "categorical":
				val, success := d.String(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "ordinal":
				val, success := d.String(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "dateTime":
				val, success := d.Int64(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "location":
				val, success := d.String(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			case "unknown":
				val, success := d.String(index)
				if !success {
					return nil, fmt.Errorf("Unabled to parse index %d as %s", index, varType)
				}
				varEntry.Set(val, varName)
				break
			default:
				log.Errorf("Unknown data type %s", varType)
			}
		}
	}

	// marshal as JSON
	return varEntry.String(), nil
}
