package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
)

const (
	defaultVarType = "text"
)

// Variable represents a single variable description.
type Variable struct {
	Name           string           `json:"varName"`
	Type           string           `json:"varType,omitempty"`
	FileType       string           `json:"varFileType,omitempty"`
	FileFormat     string           `json:"varFileFormat,omitempty"`
	Role           string           `json:"varRole,omitempty"`
	Importance     int              `json:"importance,omitempty"`
	SuggestedTypes []*SuggestedType `json:"suggestedTypes,omitempty"`
}

// SuggestedType represents a classified variable type.
type SuggestedType struct {
	Type        string  `json:"type"`
	Probability float64 `json:"probability"`
}

// Metadata represents a collection of dataset descriptions.
type Metadata struct {
	ID             string
	Name           string
	Description    string
	Summary        string
	Raw            bool
	Variables      []*Variable
	schema         *gabs.Container
	classification *gabs.Container
	NumRows        int64
	NumBytes       int64
}

// NormalizeVariableName normalizes a variable name.
func NormalizeVariableName(name string) string {
	return strings.Replace(name, ".", "_", -1)
}

// NewVariable creates a new variable.
func NewVariable(name, typ, role, fileType, fileFormat string) *Variable {
	// normalize name

	return &Variable{
		Name:       NormalizeVariableName(name),
		Type:       typ,
		Role:       role,
		FileType:   fileType,
		FileFormat: fileFormat,
	}
}

// IsRawDataset checks the schema to determine if it is a raw dataset.
func IsRawDataset(schemaPath string) (bool, error) {
	// schema file has "rawData": true | false
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return false, errors.Wrap(err, "failed to parse schema file")
	}

	isRaw, ok := schema.Path("rawData").Data().(bool)
	if !ok {
		return false, errors.Errorf("could not determine if dataset is raw")
	}

	return isRaw, nil
}

// LoadMetadataFromOriginalSchema loads metadata from a schema file.
func LoadMetadataFromOriginalSchema(schemaPath string) (*Metadata, error) {
	meta := &Metadata{}
	err := meta.loadSchema(schemaPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadName()
	if err != nil {
		return nil, err
	}
	err = meta.loadRaw()
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
	err = meta.loadOriginalSchemaVariables()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadMetadataFromMergedSchema loads metadata from a merged schema file.
func LoadMetadataFromMergedSchema(schemaPath string) (*Metadata, error) {
	meta := &Metadata{}
	err := meta.loadMergedSchema(schemaPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadName()
	if err != nil {
		return nil, err
	}
	err = meta.loadRaw()
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
	err = meta.loadMergedSchemaVariables()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadMetadataFromClassification loads metadata from a merged schema and
// classification file.
func LoadMetadataFromClassification(schemaPath string, classificationPath string) (*Metadata, error) {
	meta := &Metadata{}
	err := meta.loadMergedSchema(schemaPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadClassification(classificationPath)
	if err != nil {
		return nil, err
	}
	err = meta.loadName()
	if err != nil {
		return nil, err
	}
	err = meta.loadRaw()
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
	err = meta.loadClassificationVariables()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *Metadata) loadSchema(schemaPath string) error {
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse schema file")
	}
	m.schema = schema
	return nil
}

func (m *Metadata) loadMergedSchema(schemaPath string) error {
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse merged schema file")
	}
	// confirm merged schema
	if schema.Path("mergedSchema").Data() == nil {
		return fmt.Errorf("schema file provided is not the proper merged schema")
	}
	m.schema = schema
	return nil
}

func (m *Metadata) loadClassification(classificationPath string) error {
	classification, err := gabs.ParseJSONFile(classificationPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse classification file")
	}
	m.classification = classification
	return nil
}

// LoadImportance wiull load the importance feature selection metric.
func (m *Metadata) LoadImportance(importanceFile string, colIndices []int) error {
	// unmarshall the schema file
	importance, err := gabs.ParseJSONFile(importanceFile)
	if err != nil {
		return errors.Wrap(err, "failed to parse importance file")
	}
	// if no numeric fields, features will be null
	if importance.Path("features").Data() != nil {
		metric, err := importance.Path("features").Children()
		if err != nil {
			return errors.Wrap(err, "features attribute missing from file")
		}
		for index, col := range colIndices {
			m.Variables[col].Importance = int(metric[index].Data().(float64))
		}
	}
	return nil
}

func writeSummaryFile(summaryFile string, summary string) error {
	return ioutil.WriteFile(summaryFile, []byte(summary), 0644)
}

func (m *Metadata) setSummaryFallback() {
	if len(m.Description) < 256 {
		m.Summary = m.Description
	} else {
		m.Summary = m.Description[:256] + "..."
	}
}

func summaryAPICall(str string, lines int, apiKey string) ([]byte, error) {
	// form args
	form := url.Values{}
	form.Add("sm_api_input", str)
	// url
	url := fmt.Sprintf("http://api.smmry.com/&SM_API_KEY=%s&SM_LENGTH=%d", apiKey, lines)
	// post req
	req, err := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// client
	client := &http.Client{}
	// send it
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "Summary request failed")
	}
	defer resp.Body.Close()
	// parse response body
	return ioutil.ReadAll(resp.Body)
}

// LoadSummary loads a description summary
func (m *Metadata) LoadSummary(summaryFile string, useCache bool) error {
	// use cache if available
	if useCache {
		b, err := ioutil.ReadFile(summaryFile)
		if err == nil {
			m.Summary = string(b)
			return nil
		}
	}
	// load api key
	key := os.Getenv("SMMRY_API_KEY")
	if key == "" {
		return errors.New("SMMRY api key is missing from env var `SMMRY_API_KEY`")
	}

	// send summary API call
	body, err := summaryAPICall(m.Description, 5, key)
	if err != nil {
		return errors.Wrap(err, "failed reading summary body")
	}

	// parse response
	container, err := gabs.ParseJSON(body)
	if err != nil {
		return errors.Wrap(err, "failed parsing summary body as JSON")
	}

	// check for API error
	if container.Path("sm_api_error").Data() != nil {
		// error message
		//errStr := container.Path("sm_api_message").Data().(string)

		// fallback to description
		m.setSummaryFallback()
	} else {
		summary, ok := container.Path("sm_api_content").Data().(string)
		if !ok {
			m.setSummaryFallback()
		} else {
			m.Summary = summary
		}
	}

	// cache summary file
	writeSummaryFile(summaryFile, m.Summary)
	return nil
}

func numLines(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// LoadDatasetStats loads the dataset and computes various stats.
func (m *Metadata) LoadDatasetStats(datasetPath string) error {

	// open the left and outfiles for line-by-line by processing
	f, err := os.Open(datasetPath)
	if err != nil {
		return errors.Wrap(err, "failed to open dataset file")
	}

	fi, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to acquire stats on dataset file")
	}

	m.NumBytes = fi.Size()

	lines, err := numLines(f)
	if err != nil {
		return errors.Wrap(err, "failed to count rows in file")
	}

	m.NumRows = int64(lines)
	return nil
}

func (m *Metadata) loadRaw() error {
	isRaw, ok := m.schema.Path("rawData").Data().(bool)
	if !ok {
		m.Raw = false
	}
	m.Raw = isRaw
	return nil
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
	// load from property
	if m.schema.Path("description").Data() != nil {
		m.Description = m.schema.Path("description").Data().(string)
		return nil
	}
	// load from description file
	descPath := m.schema.Path("descriptionFile").Data().(string)
	fullDescPath := fmt.Sprintf("%s/%s", filepath.Dir(schemaPath), descPath)
	contents, err := ioutil.ReadFile(fullDescPath)
	if err != nil {
		return errors.Wrap(err, "failed to load description file")
	}
	m.Description = string(contents)
	return nil
}

func (m *Metadata) parseSchemaVariable(v *gabs.Container) (*Variable, error) {
	if v.Path("varName").Data() == nil {
		return nil, fmt.Errorf("unable to parse variable name")
	}
	varName := v.Path("varName").Data().(string)
	varType := ""
	if v.Path("varType").Data() != nil {
		varType = v.Path("varType").Data().(string)
	}
	varRole := ""
	if v.Path("varRole").Data() != nil {
		varRole = v.Path("varRole").Data().(string)
	}
	varFileType := ""
	if v.Path("varFileType").Data() != nil {
		varFileType = v.Path("varFileType").Data().(string)
	}
	varFileFormat := ""
	if v.Path("varFileFormat").Data() != nil {
		varFileFormat = v.Path("varFileFormat").Data().(string)
	}
	return NewVariable(
		varName,
		varType,
		varRole,
		varFileType,
		varFileFormat), nil
}

func (m *Metadata) parseClassification(index int, labels map[string]*gabs.Container) (string, error) {
	// parse classification
	colKey := fmt.Sprintf("%d", index)
	col, ok := labels[colKey]
	if !ok {
		return "", errors.Errorf("no label found for key `%s`", colKey)
	}
	varTypeLabels, err := col.Children()
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("failed to parse classification for column `%d`", col))
	}
	if len(varTypeLabels) > 0 {
		// TODO: fix so we don't always just use first classification
		return varTypeLabels[0].Data().(string), nil
	}
	return defaultVarType, nil
}

func (m *Metadata) parseSuggestedTypes(index int, labels map[string]*gabs.Container, probabilities map[string]*gabs.Container) ([]*SuggestedType, error) {
	// parse probabilities
	colKey := fmt.Sprintf("%d", index)
	labelsCol, ok := labels[colKey]
	if !ok {
		return nil, errors.Errorf("no label found for key `%s`", colKey)
	}
	probabilitiesCol, ok := probabilities[colKey]
	if !ok {
		return nil, errors.Errorf("no probabilities found for key `%s`", colKey)
	}
	varTypeLabels, err := labelsCol.Children()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to parse classification for column `%d`", labelsCol))
	}
	varProbabilities, err := probabilitiesCol.Children()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to parse probabilities for column `%d`", probabilitiesCol))
	}
	var suggested []*SuggestedType
	for index, label := range varTypeLabels {
		prob := varProbabilities[index]
		suggested = append(suggested, &SuggestedType{
			Type:        label.Data().(string),
			Probability: prob.Data().(float64),
		})
	}
	return suggested, nil
}

func (m *Metadata) loadOriginalSchemaVariables() error {
	trainVariables, err := m.schema.Path("trainData.trainData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse training data")
	}
	targetVariables, err := m.schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse target data")
	}
	schemaVariables := m.mergeVariables(trainVariables, targetVariables)
	for _, v := range schemaVariables {
		variable, err := m.parseSchemaVariable(v)
		if err != nil {
			return err
		}
		m.Variables = append(m.Variables, variable)
	}
	return nil
}

func (m *Metadata) loadMergedSchemaVariables() error {
	schemaVariables, err := m.schema.Path("mergedData.mergedData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse training data")
	}
	for _, v := range schemaVariables {
		variable, err := m.parseSchemaVariable(v)
		if err != nil {
			return err
		}
		m.Variables = append(m.Variables, variable)
	}
	return nil
}

func (m *Metadata) loadClassificationVariables() error {
	schemaVariables, err := m.schema.Path("mergedData.mergedData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse merged data")
	}
	labels, err := m.classification.Path("labels").ChildrenMap()
	if err != nil {
		return errors.Wrap(err, "failed to parse classification labels")
	}

	probabilities, err := m.classification.Path("label_probabilities").ChildrenMap()
	if err != nil {
		return errors.Wrap(err, "Unable to parse classification probabilities")
	}

	for index, v := range schemaVariables {
		variable, err := m.parseSchemaVariable(v)
		if err != nil {
			return err
		}
		typ, err := m.parseClassification(index, labels)
		if err != nil {
			return err
		}

		suggestedTypes, err := m.parseSuggestedTypes(index, labels, probabilities)
		if err != nil {
			return err
		}
		// override type with classification / probabilities
		variable.Type = typ
		variable.SuggestedTypes = suggestedTypes
		m.Variables = append(m.Variables, variable)
	}
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

// WriteMergedSchema exports the current meta data as a merged schema file.
func (m *Metadata) WriteMergedSchema(path string) error {
	// create output format
	output := map[string]interface{}{
		"datasetId":    m.ID,
		"description":  m.Description,
		"rawData":      m.Raw,
		"mergedSchema": "true",
		"mergedData": map[string]interface{}{
			"mergedData": m.Variables,
		},
	}
	bytes, err := json.MarshalIndent(output, "", "    ")
	if err != nil {
		return errors.Wrap(err, "failed to marshal merged schema file output")
	}
	// write copy to disk
	return ioutil.WriteFile(path, bytes, 0644)
}

// IngestMetadata adds a document consisting of the metadata to the
// provided index.
func IngestMetadata(client *elastic.Client, index string, meta *Metadata) error {

	// filter variables for surce object
	var vars []*Variable
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
		"summary":     meta.Summary,
		"numRows":     meta.NumRows,
		"numBytes":    meta.NumBytes,
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
					"summary": {
						"type": "text"
					},
					"numRows": {
						"type": "long"
					},
					"numBytes": {
						"type": "long"
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
							},
							"importance": {
								"type": "integer"
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
