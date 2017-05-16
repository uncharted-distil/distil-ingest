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

func parseAndSetVal(index int, varType string, varName string, varEntry *gabs.Container, parser func() (interface{}, bool)) error {
	val, success := parser()
	if !success {
		return fmt.Errorf("Unabled to parse index %d as %s", index, varType)
	}
	varEntry.Set(val, varName)
	return nil
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

// GetMapping returns the document's mappings.
func (d *D3MData) GetMapping() (string, error) {
	// grab the variable description portion of the schema
	trainingArray, err := d.schema.Path("trainData.trainData").Children()
	if err != nil {
		log.Error(err)
		return "", err
	}

	// create the ES mappings based on the variables in the schema
	mappings := gabs.New()
	for _, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})
		var varType string
		if varDesc["varRole"].(string) == "attribute" {
			switch varDesc["varType"].(string) {
			case "integer":
				varType = "long"
				break
			case "float":
				varType = "double"
				break
			case "text":
				varType = "text"
				break
			case "categorical":
				varType = "text"
				break
			case "ordinal":
				varType = "text"
				break
			case "unknown":
				varType = "text"
				break
			case "dateTime":
				varType = "date" // for now
				break
			case "location":
				varType = "text" // for now
				break
			default:
				log.Errorf("Unknown data type %s", varType)
			}
			mappings.SetP(varType, "datum.properties."+varDesc["varName"].(string)+".type")
		}
	}

	return mappings.String(), nil
}

// GetSource returns the source document in JSON format
func (d *D3MData) GetSource() (interface{}, error) {
	// grab the variable description portion of the schema
	trainingArray, err := d.schema.Path("trainData.trainData").Children()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// iterate over the variable descriptions
	varEntry := gabs.New()
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
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.Int64(index) })
				break
			case "float":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.Float64(index) })
				break
			case "text":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.String(index) })
				break
			case "categorical":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.String(index) })
				break
			case "ordinal":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.String(index) })
				break
			case "dateTime":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.Int64(index) })
				break
			case "location":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.String(index) })
				break
			case "unknown":
				parseAndSetVal(index, varType, varName, varEntry, func() (interface{}, bool) { return d.String(index) })
				break
			default:
				log.Errorf("Unknown data type %s", varType)
			}
		}
	}

	// marshal as JSON
	return varEntry.String(), nil
}
