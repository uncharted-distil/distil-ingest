package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"

	"github.com/unchartedsoftware/distil-ingest/rest"
	"github.com/unchartedsoftware/distil-ingest/smmry"
)

const (
	defaultVarType        = "text"
	variableNameSizeLimit = 50
)

var (
	nameRegex = regexp.MustCompile("[^a-zA-Z0-9]")
)

// Variable represents a single variable description.
type Variable struct {
	Name           string           `json:"colName"`
	Type           string           `json:"colType,omitempty"`
	FileType       string           `json:"colFileType,omitempty"`
	FileFormat     string           `json:"colFileFormat,omitempty"`
	SelectedRole   string           `json:"selectedRole,omitempty"`
	Role           []string         `json:"role,omitempty"`
	OriginalName   string           `json:"colOriginalName,omitempty"`
	DisplayName    string           `json:"colDisplayName,omitempty"`
	Importance     int              `json:"importance,omitempty"`
	Index          int              `json:"colIndex,omitempty"`
	SuggestedTypes []*SuggestedType `json:"suggestedTypes,omitempty"`
	RefersTo       *gabs.Container
}

// DataResource represents a set of variables found in a data asset.
type DataResource struct {
	ResID     string `json:"resID"`
	ResPath   string `json:"resPath"`
	Variables []*Variable
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
	SummaryMachine string
	Raw            bool
	DataResources  []*DataResource
	schema         *gabs.Container
	classification *gabs.Container
	NumRows        int64
	NumBytes       int64
}

// NormalizeVariableName normalizes a variable name.
func NormalizeVariableName(name string) string {
	nameNormalized := nameRegex.ReplaceAllString(name, "_")
	if len(nameNormalized) > variableNameSizeLimit {
		nameNormalized = nameNormalized[:variableNameSizeLimit]
	}

	return nameNormalized
}

// NewVariable creates a new variable.
func NewVariable(index int, name, typ, fileType, fileFormat string, role []string, refersTo *gabs.Container, existingVariables []*Variable, normalizeName bool) *Variable {
	normed := name
	if normalizeName {
		// normalize name
		normed = NormalizeVariableName(name)

		// normed name needs to be unique
		count := 0
		for _, v := range existingVariables {
			if v.Name == normed {
				count = count + 1
			}
		}
		if count > 0 {
			normed = fmt.Sprintf("%s_%d", normed, count)
		}
	}

	// select the first role by default.
	selectedRole := ""
	if len(role) > 0 {
		selectedRole = role[0]
	}

	return &Variable{
		Name:         normed,
		Index:        index,
		Type:         typ,
		Role:         role,
		SelectedRole: selectedRole,
		OriginalName: normed,
		DisplayName:  name,
		FileType:     fileType,
		FileFormat:   fileFormat,
		RefersTo:     refersTo,
	}
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
	err = meta.loadID()
	if err != nil {
		return nil, err
	}
	err = meta.loadDescription()
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
	err = meta.loadID()
	if err != nil {
		return nil, err
	}
	err = meta.loadDescription()
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
	err = meta.loadID()
	if err != nil {
		return nil, err
	}
	err = meta.loadDescription()
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
	if schema.Path("about.mergedSchema").Data() == nil {
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
	// NOTE: Assume all variables in a single resource since that is
	// how we would submit to ranking.
	if importance.Path("features").Data() != nil {
		metric, err := importance.Path("features").Children()
		if err != nil {
			return errors.Wrap(err, "features attribute missing from file")
		}
		for index, col := range colIndices {
			m.DataResources[0].Variables[col].Importance = int(metric[index].Data().(float64))
		}
	}
	return nil
}

func writeSummaryFile(summaryFile string, summary string) error {
	return ioutil.WriteFile(summaryFile, []byte(summary), 0644)
}

// GenerateHeaders generates csv headers for the data resources.
func (m *Metadata) GenerateHeaders() ([][]string, error) {
	// each data resource needs a separate header
	headers := make([][]string, len(m.DataResources))

	for index, dr := range m.DataResources {
		header := make([]string, len(dr.Variables))

		// iterate over the fields
		for hIndex, field := range dr.Variables {
			header[hIndex] = strings.Replace(field.Name, "_", "", -1)
		}

		headers[index] = header
	}

	return headers, nil
}

// LoadSummaryFromDescription loads a summary from the description.
func (m *Metadata) LoadSummaryFromDescription(summaryFile string) error {
	// request summary
	summary, err := smmry.GetSummaryFromDescription(m.Description)
	if err != nil {
		return err
	}
	// set summary
	m.Summary = summary
	// cache summary file
	writeSummaryFile(summaryFile, m.Summary)
	return nil
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
	return m.LoadSummaryFromDescription(summaryFile)
}

// LoadSummaryMachine loads a machine-learned summary.
func (m *Metadata) LoadSummaryMachine(summaryFile string) error {
	b, err := ioutil.ReadFile(summaryFile)
	if err != nil {
		return errors.Wrap(err, "unable to read machine-learned summary")
	}

	summary := &rest.SummaryResult{}
	err = json.Unmarshal(b, summary)
	if err != nil {
		return errors.Wrap(err, "unable to parse machine-learned summary")
	}

	m.SummaryMachine = summary.Summary

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

func (m *Metadata) loadID() error {
	id, ok := m.schema.Path("about.datasetID").Data().(string)
	if !ok {
		return errors.Errorf("no `about.datasetID` key found in schema")
	}
	m.ID = id
	return nil
}

func (m *Metadata) loadName() error {
	name, ok := m.schema.Path("about.datasetName").Data().(string)
	if !ok {
		return nil //errors.Errorf("no `name` key found in schema")
	}
	m.Name = name
	return nil
}

func (m *Metadata) loadDescription() error {
	// load from property
	if m.schema.Path("about.description").Data() != nil {
		m.Description = m.schema.Path("about.description").Data().(string)
		return nil
	}
	return nil
}

func (m *Metadata) parseSchemaVariable(v *gabs.Container, existingVariables []*Variable, normalizeName bool) (*Variable, error) {
	if v.Path("colName").Data() == nil {
		return nil, fmt.Errorf("unable to parse column name")
	}
	varName := v.Path("colName").Data().(string)

	varType := ""
	if v.Path("colType").Data() != nil {
		varType = v.Path("colType").Data().(string)
	}

	varIndex := 0
	if v.Path("colIndex").Data() != nil {
		varIndex = int(v.Path("colIndex").Data().(float64))
	}

	var varRoles []string
	if v.Path("role").Data() != nil {
		rolesRaw, err := v.Path("role").Children()
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse column role")
		}
		varRoles = make([]string, len(rolesRaw))
		for i, r := range rolesRaw {
			varRoles[i] = r.Data().(string)
		}
	}

	varFileType := ""
	if v.Path("varFileType").Data() != nil {
		varFileType = v.Path("varFileType").Data().(string)
	}

	varFileFormat := ""
	if v.Path("varFileFormat").Data() != nil {
		varFileFormat = v.Path("varFileFormat").Data().(string)
	}

	var refersTo *gabs.Container
	if v.Path("refersTo").Data() != nil {
		refersTo = v.Path("refersTo")
	}
	return NewVariable(
		varIndex,
		varName,
		varType,
		varFileType,
		varFileFormat,
		varRoles,
		refersTo,
		existingVariables,
		normalizeName), nil
}

func (m *Metadata) cleanVarType(name string, typ string) string {
	// set the d3m index to int regardless of what gets returned
	if name == "d3mIndex" {
		return "index"
	}
	// map types
	switch typ {
	case "int":
		return "integer"
	default:
		return typ
	}
}

func (m *Metadata) parseClassification(index int, labels []*gabs.Container) (string, error) {
	// parse classification
	col := labels[index]
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

func (m *Metadata) parseSuggestedTypes(name string, index int, labels []*gabs.Container, probabilities []*gabs.Container) ([]*SuggestedType, error) {
	// parse probabilities
	labelsCol := labels[index]
	probabilitiesCol := probabilities[index]
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
		typ := label.Data().(string)
		probability := prob.Data().(float64)
		suggested = append(suggested, &SuggestedType{
			Type:        m.cleanVarType(name, typ),
			Probability: probability,
		})
	}
	// sort by probability
	sort.Slice(suggested, func(i, j int) bool {
		return suggested[i].Probability > suggested[j].Probability
	})
	return suggested, nil
}

func (m *Metadata) loadOriginalSchemaVariables() error {
	dataResources, err := m.schema.Path("dataResources").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse data resources")
	}

	// Parse the variables for every schema
	m.DataResources = make([]*DataResource, len(dataResources))
	for i, sv := range dataResources {
		schemaVariables, err := sv.Path("columns").Children()
		if err != nil {
			return errors.Wrap(err, "failed to parse column data")
		}

		if sv.Path("resID").Data() == nil {
			return fmt.Errorf("unable to parse resource id")
		}
		resID := sv.Path("resID").Data().(string)

		if sv.Path("resPath").Data() == nil {
			return fmt.Errorf("unable to parse resource path")
		}
		resPath := sv.Path("resPath").Data().(string)

		m.DataResources[i] = &DataResource{
			ResID:     resID,
			ResPath:   resPath,
			Variables: make([]*Variable, 0),
		}

		for _, v := range schemaVariables {
			variable, err := m.parseSchemaVariable(v, m.DataResources[i].Variables, false)
			if err != nil {
				return err
			}
			m.DataResources[i].Variables = append(m.DataResources[i].Variables, variable)
		}
	}
	return nil
}

func (m *Metadata) loadMergedSchemaVariables() error {
	schemaVariables, err := m.schema.Path("mergedData.mergedData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse merged schema")
	}

	// Merged schema has only one set of variables
	m.DataResources = make([]*DataResource, 1)
	m.DataResources[0] = &DataResource{
		Variables: make([]*Variable, 0),
	}

	for _, v := range schemaVariables {
		variable, err := m.parseSchemaVariable(v, m.DataResources[0].Variables, true)
		if err != nil {
			return errors.Wrap(err, "failed to parse merged schema variable")
		}
		m.DataResources[0].Variables = append(m.DataResources[0].Variables, variable)
	}
	return nil
}

func (m *Metadata) loadClassificationVariables() error {
	schemaVariables, err := m.schema.Path("mergedData.mergedData").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse merged data")
	}

	labels, err := m.classification.Path("labels").Children()
	if err != nil {
		return errors.Wrap(err, "failed to parse classification labels")
	}

	probabilities, err := m.classification.Path("label_probabilities").Children()
	if err != nil {
		return errors.Wrap(err, "Unable to parse classification probabilities")
	}

	// All variables now in a single dataset since it is merged
	m.DataResources = make([]*DataResource, 1)
	m.DataResources[0] = &DataResource{
		Variables: make([]*Variable, 0),
	}

	for index, v := range schemaVariables {
		variable, err := m.parseSchemaVariable(v, m.DataResources[0].Variables, true)
		if err != nil {
			return err
		}
		// get suggested types
		suggestedTypes, err := m.parseSuggestedTypes(variable.Name, index, labels, probabilities)
		if err != nil {
			return err
		}
		variable.SuggestedTypes = suggestedTypes
		// set type to that with highest probability
		if len(suggestedTypes) > 0 {
			variable.Type = suggestedTypes[0].Type
		} else {
			variable.Type = defaultVarType
		}
		m.DataResources[0].Variables = append(m.DataResources[0].Variables, variable)
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
func (m *Metadata) WriteMergedSchema(path string, mergedDataResource *DataResource) error {
	// create output format
	output := map[string]interface{}{
		"about": map[string]interface{}{
			"datasetID":    m.ID,
			"datasetName":  m.Name,
			"description":  m.Description,
			"rawData":      m.Raw,
			"mergedSchema": "true",
		},
		"mergedData": map[string]interface{}{
			"mergedData": mergedDataResource.Variables,
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
func IngestMetadata(client *elastic.Client, index string, datasetPrefix string, meta *Metadata) error {

	// filter variables for surce object
	if len(meta.DataResources) > 1 {
		return errors.New("metadata variables not merged into a single dataset")
	}
	adjustedID := datasetPrefix + meta.ID

	source := map[string]interface{}{
		"datasetName":    meta.Name,
		"datasetID":      adjustedID,
		"description":    meta.Description,
		"summary":        meta.Summary,
		"summaryMachine": meta.SummaryMachine,
		"numRows":        meta.NumRows,
		"numBytes":       meta.NumBytes,
		"variables":      meta.DataResources[0].Variables,
	}

	bytes, err := json.Marshal(source)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal document source")
	}

	// push the document into the metadata index
	_, err = client.Index().
		Index(index).
		Type("metadata").
		Id(adjustedID).
		BodyString(string(bytes)).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to add document to index `%s`", index)
	}
	return nil
}

func (m *Metadata) DatasetMatches(variables []string) bool {
	// Assume metadata is for a merged schema, so only has 1 data resource.

	// Lengths need to be the same.
	if len(variables) != len(m.DataResources[0].Variables) {
		return false
	}

	// Build the variable lookup for matching.
	newVariable := make(map[string]bool)
	for _, v := range variables {
		newVariable[v] = true
	}

	// Make sure every existing variable is present.
	for _, v := range m.DataResources[0].Variables {
		if !newVariable[v.Name] {
			return false
		}
	}

	// Same amount of varibles, and all the names match.
	return true
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
					"summaryMachine": {
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
							"varOriginalName": {
								"type": "text"
							},
							"varDisplayName": {
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
