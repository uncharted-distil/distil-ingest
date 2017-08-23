package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
)

const (
	defaultVarType = "text"
)

// Variable represents a single variable description.
type Variable struct {
	Name string `json:"varName"`
	Type string `json:"varType"`
	Role string `json:"varRole"`
}

// Metadata represents a collection of dataset descriptions.
type Metadata struct {
	ID             string
	Name           string
	Description    string
	Variables      []Variable
	schema         *gabs.Container
	classification *gabs.Container
}

// LoadMetadataFromSchema loads metadata from a single schema file.
func LoadMetadataFromSchema(schemaPath string) (*Metadata, error) {
	// unmarshall the schema file
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse schema file")
	}
	meta := &Metadata{
		schema: schema,
	}
	err = meta.loadName()
	if err != nil {
		return nil, err
	}
	err = meta.loadID()
	if err != nil {
		return nil, err
	}
	err = meta.loadDescription(schemaPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadVariables()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadMetadataFromClassification loads metadata from a schema and
// classification file.
func LoadMetadataFromClassification(schemaPath string, classificationPath string) (*Metadata, error) {
	// unmarshall the schema file
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse schema file")
	}
	// unmarshall the classification file
	classification, err := gabs.ParseJSONFile(classificationPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse classification file")
	}
	meta := &Metadata{
		schema:         schema,
		classification: classification,
	}
	err = meta.loadName()
	if err != nil {
		return nil, err
	}
	err = meta.loadID()
	if err != nil {
		return nil, err
	}
	err = meta.loadDescription(schemaPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadVariables()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *Metadata) loadID() error {
	id, ok := m.schema.Path("datasetId").Data().(string)
	if !ok {
		return errors.Errorf("no `datasetId` key found in schema")
	}
	m.ID = id
	return nil
}

func (m *Metadata) loadName() error {
	name, ok := m.schema.Path("name").Data().(string)
	if !ok {
		return nil //errors.Errorf("no `name` key found in schema")
	}
	m.Name = name
	return nil
}

func (m *Metadata) loadDescription(schemaPath string) error {
	descPath := m.schema.Path("descriptionFile").Data().(string)
	fullDescPath := fmt.Sprintf("%s/%s", filepath.Dir(schemaPath), descPath)
	contents, err := ioutil.ReadFile(fullDescPath)
	if err != nil {
		return errors.Wrap(err, "failed to load description file")
	}
	m.Description = string(contents)
	return nil
}

func (m *Metadata) loadVariables() error {
	// add the training and target data variables - - don't include the index
	// columns in the final values
	trainVariables, err := m.schema.Path("trainData.trainData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse training data")
	}
	targetVariables, err := m.schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse target data")
	}
	schemaVariables := m.mergeVariables(trainVariables, targetVariables)

	var varTypes []string

	if m.classification != nil {
		// get variable types from classification
		labels, err := m.classification.Path("labels").ChildrenMap()
		if err != nil {
			return errors.Wrap(err, "failed to parse classification types")
		}

		for index := range schemaVariables {
			colKey := fmt.Sprintf("%d", index)
			col, ok := labels[colKey]
			if !ok {
				return errors.Errorf("no column found for key `%s`", colKey)
			}
			varTypeLabels, err := col.Children()
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to parse varType for column `%d`", col))
			}
			varType := defaultVarType
			if len(varTypeLabels) > 0 {
				// TODO: fix so we don't always just use first classification
				varType = varTypeLabels[0].Data().(string)
			}
			varTypes = append(varTypes, varType)
		}

	} else {
		// get variable types from schema
		for _, v := range schemaVariables {
			varType := v.Path("varType").Data().(string)
			varTypes = append(varTypes, varType)
		}
	}

	var variables []Variable

	for index, v := range schemaVariables {
		varRole := v.Path("varRole").Data().(string)
		varName := v.Path("varName").Data().(string)
		variables = append(variables, Variable{
			Name: varName,
			Type: varTypes[index],
			Role: varRole,
		})
	}

	m.Variables = variables
	return nil
}

func (m *Metadata) mergeVariables(left []*gabs.Container, right []*gabs.Container) []*gabs.Container {
	var res []*gabs.Container
	added := make(map[string]bool)
	for _, val := range left {
		name := val.Path("varName").Data().(string)
		_, ok := added[name]
		if ok {
			continue
		}
		res = append(res, val)
		added[name] = true
	}
	for _, val := range right {
		name := val.Path("varName").Data().(string)
		_, ok := added[name]
		if ok {
			continue
		}
		res = append(res, val)
		added[name] = true
	}
	return res
}

// IngestMetadata adds a document consisting of the metadata to the
// provided index.
func IngestMetadata(client *elastic.Client, index string, meta *Metadata) error {

	// filter variables for surce object
	var vars []Variable
	for _, v := range meta.Variables {
		// exclude index
		if v.Role != "index" {
			vars = append(vars, v)
		}
	}
	source := map[string]interface{}{
		"name":        meta.Name,
		"datasetId":   meta.ID,
		"description": meta.Description,
		"variables":   vars,
	}

	bytes, err := json.Marshal(source)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal document source")
	}

	// push the document into the metadata index
	_, err = client.Index().
		Index(index).
		Type("metadata").
		Id(meta.ID).
		BodyString(string(bytes)).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to add document to index `%s`", index)
	}
	return nil
}

// CreateMetadataIndex creates a new ElasticSearch index with our target
// mappings. An ngram analyze is defined and applied to the variable names to
// allow for substring searching.
func CreateMetadataIndex(client *elastic.Client, index string, overwrite bool) error {
	// check if it already exists
	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to complete check for existence of index %s", index)
	}

	// delete the index if it already exists
	if exists {
		if overwrite {
			deleted, err := client.DeleteIndex(index).Do(context.Background())
			if err != nil {
				return errors.Wrapf(err, "failed to delete index %s", index)
			}
			if !deleted.Acknowledged {
				return fmt.Errorf("failed to create index `%s`, index could not be deleted", index)
			}
		} else {
			return nil
		}
	}

	// create body
	body := `{
		"settings": {
			"analysis": {
				"filter": {
					"ngram_filter": {
						"type": "ngram",
						"min_gram": 4,
						"max_gram": 20
					}
				},
				"analyzer": {
					"ngram_analyzer": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": [
							"lowercase",
							"ngram_filter"
						]
					}
				}
			}
		},
		"mappings": {
			"metadata": {
				"properties": {
					"datasetId": {
						"type": "text"
					},
					"name": {
						"type": "text"
					},
					"description": {
						"type": "text"
					},
					"variables": {
						"properties": {
							"varDescription": {
								"type": "text"
							},
							"varName": {
								"type": "text",
								"analyzer": "ngram_analyzer",
								"include_in_all": true,
								"term_vector": "yes"
							},
							"varRole": {
								"type": "text"
							},
							"varType": {
								"type": "text"
							}
						}
					}
				}
			}
		}
	}`

	// create index
	created, err := client.CreateIndex(index).BodyString(body).Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to create index %s", index)
	}
	if !created.Acknowledged {
		return fmt.Errorf("Failed to create new index %s", index)
	}
	return nil
}
