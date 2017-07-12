package metadata

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
)

// CreateMetadataIndex creates a new ElasticSearch index with our target mappings.  An ngram analyzer
// is defined and applied to the variable names to allow for substring searching.
func CreateMetadataIndex(index string, overwrite bool, client *elastic.Client) error {
	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to complete check for existence of index %s", index)
	}

	// delete the index if it already exists
	if exists {
		if !overwrite {
			return fmt.Errorf("failed to create index `%s`, already exists and overwrite is disabled", index)
		}

		deleted, err := client.DeleteIndex(index).Do(context.Background())
		if err != nil {
			return errors.Wrapf(err, "failed to delete index %s", index)
		}

		if !deleted.Acknowledged {
			return fmt.Errorf("failed to create index `%s`, index could not be deleted", index)
		}
	}

	creationData := `{
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
	created, err := client.CreateIndex(index).BodyString(creationData).Do(context.Background())
	if err != nil {
		errors.Wrapf(err, "failed to create index %s", index)
	}
	if !created.Acknowledged {
		return fmt.Errorf("Failed to create new index %s", index)
	}
	return nil
}

// IngestMetadata adds a document consisting of the dataset's metadata to the
// caller supplied index.
func IngestMetadata(index string, schemaPath string, client *elastic.Client) error {
	// Unmarshall the schema file
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return errors.Wrap(err, "failed to parses schema file")
	}

	// load data description text
	descPath := schema.Path("descriptionFile").Data().(string)
	contents, err := ioutil.ReadFile(filepath.Dir(schemaPath) + "/" + descPath)
	if err != nil {
		return errors.Wrap(err, "failed to load description file")
	}

	// create a new object for our output metadata and write the parts of the schema
	// we want into it - name, id, description, variable info
	output := gabs.New()
	val, ok := schema.Path("name").Data().(string)
	if ok {
		output.SetP(val, "name")
	}
	output.SetP(schema.Path("datasetId").Data().(string), "datasetId")
	output.SetP(string(contents), "description")
	output.ArrayP("variables")

	// add the training and target data variables - - don't include the index columns in the final
	// values
	trainVariables, err := schema.Path("trainData.trainData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse training data")
	}
	targetVariables, err := schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse target data")
	}
	variables := append(trainVariables, targetVariables...)

	for _, variable := range variables {
		if variable.Path("varRole").Data().(string) == "index" {
			continue
		}
		output.ArrayAppendP(variable.Data(), "variables")
	}

	id := schema.Path("datasetId").Data().(string)

	// push the document into the metadata index
	indexResp, err := client.Index().
		Index(index).
		Type("metadata").
		Id(id).
		BodyString(output.String()).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to add document to index `%s`", index)
	}

	if !indexResp.Created {
		return fmt.Errorf("failed to add new metadata record with ID `%s`", id)
	}

	return nil
}
