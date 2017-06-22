package d3mdata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"
	"github.com/unchartedsoftware/plog"
)

// D3MData is a row from a CSV file
type D3MData struct {
	document.CSV
	schema *gabs.Container
	idCol  int
}

func parseAndSetVal(index int, varType string, varName string, varEntry *gabs.Container, parser func() (interface{}, bool)) error {
	val, success := parser()
	if !success {
		return fmt.Errorf("Unabled to parse index %d as %s", index, varType)
	}
	varEntry.SetP(val, varName+".value")
	varEntry.SetP(varType, varName+".schemaType")
	return nil
}

// NewD3MData instantiates and returns a new document.
func NewD3MData(schemaPath string) (deluge.Constructor, error) {
	// Unmarshall the schema file
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse schema file")
	}

	// find the row ID column and store it for quick retrieval
	trainingArray, err := schema.Path("trainData.trainData").Children()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse train data")
	}
	var idCol int
	for index, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})
		if varDesc["varName"].(string) == "d3mIndex" {
			idCol = index
			break
		}
	}

	return func() (deluge.Document, error) {
		return &D3MData{schema: schema, idCol: idCol}, nil
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
	// grab the variable description portion of the schema
	trainingArray, err := d.schema.Path("trainData.trainData").Children()
	if err != nil {
		return "", errors.Wrap(err, "Failed to parse train data")
	}
	targetArray, err := d.schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return "", errors.Wrap(err, "Failed to parse target data")
	}
	trainingArray = append(trainingArray, targetArray...)

	// create the ES mappings based on the variables in the schema
	mappings := gabs.New()
	for _, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})
		var varType string
		role := varDesc["varRole"].(string)
		if role == "attribute" || role == "target" {
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
			mappings.SetP(varType, "datum.properties."+varDesc["varName"].(string)+".properties.value.type")
			mappings.SetP("text", "datum.properties."+varDesc["varName"].(string)+".properties.schemaType.type")
		}
	}

	return mappings.String(), nil
}

// GetSource returns the source document in JSON format
func (d *D3MData) GetSource() (interface{}, error) {
	// grab the variable description portion of the schema for the training data
	trainingArray, err := d.schema.Path("trainData.trainData").Children()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse train data")
	}
	// do the same for the training targets
	targetArray, err := d.schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse target data")
	}
	// strip the index info out of the training targets sesction - the merged csv
	// being ingested doesn't contain that column
	var indexCol = 0
	for index, child := range targetArray {
		varDesc := child.Data().(map[string]interface{})
		if varDesc["varRole"].(string) == "index" {
			indexCol = index
			break
		}
	}
	targetArray = append(targetArray[:indexCol], targetArray[indexCol+1:]...)

	// process both lists
	trainingArray = append(trainingArray, targetArray...)

	// iterate over the variable descriptions
	varEntry := gabs.New()
	for index, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})

		// ignore anything other than attributes
		role := varDesc["varRole"].(string)
		if role == "attribute" || role == "target" {
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
