//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package metadata

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-ingest/pkg/rest"
	"github.com/uncharted-distil/distil-ingest/pkg/smmry"
)

// DatasetSource flags the type of ingest action that created a dataset
type DatasetSource string

const (
	// ProvenanceSimon identifies the type provenance as Simon
	ProvenanceSimon = "d3m.primitives.distil.simon"
	// ProvenanceSchema identifies the type provenance as schema
	ProvenanceSchema = "schema"

	schemaVersion = "3.1.1"
	license       = "Unknown"

	// Seed flags a dataset as ingested from seed data
	Seed DatasetSource = "seed"

	// Contrib flags a dataset as being ingested from contributed data
	Contrib DatasetSource = "contrib"

	// Augmented flags a dataset as being ingested from augmented data
	Augmented DatasetSource = "augmented"
)

var (
	typeProbabilityThreshold = 0.8
)

type classificationData struct {
	labels        []*gabs.Container
	probabilities []*gabs.Container
}

// SetTypeProbabilityThreshold below which a suggested type is not used as
// variable type
func SetTypeProbabilityThreshold(threshold float64) {
	typeProbabilityThreshold = threshold
}

// IsMetadataVariable indicates whether or not a variable is additional metadata
// added to the source.
func IsMetadataVariable(v *model.Variable) bool {
	return strings.HasPrefix(v.Name, "_")
}

// LoadMetadataFromOriginalSchema loads metadata from a schema file.
func LoadMetadataFromOriginalSchema(schemaPath string) (*model.Metadata, error) {
	meta := &model.Metadata{
		SchemaSource: model.SchemaSourceOriginal,
	}
	err := loadSchema(meta, schemaPath)
	if err != nil {
		return nil, err
	}
	err = loadName(meta)
	if err != nil {
		return nil, err
	}
	err = loadID(meta)
	if err != nil {
		return nil, err
	}
	err = loadAbout(meta)
	if err != nil {
		return nil, err
	}
	err = loadOriginalSchemaVariables(meta, schemaPath)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadMetadataFromMergedSchema loads metadata from a merged schema file.
func LoadMetadataFromMergedSchema(schemaPath string) (*model.Metadata, error) {
	meta := &model.Metadata{
		SchemaSource: model.SchemaSourceMerged,
	}
	err := loadMergedSchema(meta, schemaPath)
	if err != nil {
		return nil, err
	}
	err = loadName(meta)
	if err != nil {
		return nil, err
	}
	err = loadID(meta)
	if err != nil {
		return nil, err
	}
	err = loadAbout(meta)
	if err != nil {
		return nil, err
	}
	err = loadMergedSchemaVariables(meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadMetadataFromRawFile loads metadata from a raw file
// and a classification file.
func LoadMetadataFromRawFile(datasetPath string, classificationPath string) (*model.Metadata, error) {
	directory := filepath.Dir(datasetPath)
	directory = filepath.Base(directory)
	meta := &model.Metadata{
		ID:           directory,
		Name:         directory,
		StorageName:  model.NormalizeDatasetID(directory),
		SchemaSource: model.SchemaSourceRaw,
	}

	dr, err := loadRawVariables(datasetPath)
	if err != nil {
		return nil, err
	}
	meta.DataResources = []*model.DataResource{dr}

	if classificationPath != "" {
		classification, err := loadClassification(classificationPath)
		if err != nil {
			return nil, err
		}
		meta.Classification = classification

		err = addClassificationTypes(meta, classificationPath)
		if err != nil {
			return nil, err
		}
	}

	return meta, nil
}

// LoadMetadataFromClassification loads metadata from a merged schema and
// classification file.
func LoadMetadataFromClassification(schemaPath string, classificationPath string, normalizeVariableNames bool) (*model.Metadata, error) {
	meta := &model.Metadata{
		SchemaSource: model.SchemaSourceClassification,
	}

	// If classification can't be loaded, try to load from merged schema.
	classification, err := loadClassification(classificationPath)
	if err != nil {
		log.Warnf("unable to load classification file: %v", err)
		log.Warnf("attempting to load from merged schema")
		return LoadMetadataFromMergedSchema(schemaPath)
	}
	meta.Classification = classification

	err = loadMergedSchema(meta, schemaPath)
	if err != nil {
		return nil, err
	}
	err = loadName(meta)
	if err != nil {
		return nil, err
	}
	err = loadID(meta)
	if err != nil {
		return nil, err
	}
	err = loadAbout(meta)
	if err != nil {
		return nil, err
	}
	err = loadClassificationVariables(meta, normalizeVariableNames)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func parseClassificationFile(classificationPath string) (*classificationData, error) {
	classification, err := loadClassification(classificationPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load classification")
	}

	labels := classification.Path("labels").Children()
	if labels == nil {
		return nil, errors.New("failed to parse classification labels")
	}

	probabilities := classification.Path("label_probabilities").Children()
	if probabilities == nil {
		return nil, errors.New("Unable to parse classification probabilities")
	}

	return &classificationData{
		labels:        labels,
		probabilities: probabilities,
	}, nil
}

func addClassificationTypes(m *model.Metadata, classificationPath string) error {
	classification, err := parseClassificationFile(classificationPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse classification file")
	}

	for index, variable := range m.DataResources[0].Variables {
		// get suggested types
		suggestedTypes, err := parseSuggestedTypes(m, variable.Name, index, classification.labels, classification.probabilities)
		if err != nil {
			return err
		}
		variable.SuggestedTypes = append(variable.SuggestedTypes, suggestedTypes...)
		// set type to that with highest probability
		if len(variable.SuggestedTypes) > 0 && variable.SuggestedTypes[0].Probability >= typeProbabilityThreshold {
			variable.Type = variable.SuggestedTypes[0].Type
		} else {
			variable.Type = model.DefaultVarType
		}
	}

	return nil
}

func loadRawVariables(datasetPath string) (*model.DataResource, error) {
	// read header from the raw datafile.
	csvFile, err := os.Open(datasetPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open raw data file")
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	fields, err := reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read header line")
	}

	// All variables now in a single dataset since it is merged
	dataResource := &model.DataResource{
		Variables: make([]*model.Variable, 0),
	}

	for index, v := range fields {
		variable := model.NewVariable(
			index,
			v,
			"",
			"",
			"",
			"",
			"",
			[]string{"attribute"},
			model.VarRoleData,
			nil,
			dataResource.Variables,
			false)
		variable.Type = model.StringType
		dataResource.Variables = append(dataResource.Variables, variable)
	}
	return dataResource, nil
}

func loadSchema(m *model.Metadata, schemaPath string) error {
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse schema file")
	}
	m.Schema = schema
	return nil
}

func loadMergedSchema(m *model.Metadata, schemaPath string) error {
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return errors.Wrap(err, "failed to parse merged schema file")
	}
	// confirm merged schema
	if schema.Path("about.mergedSchema").Data() == nil {
		return fmt.Errorf("schema file provided is not the proper merged schema")
	}
	m.Schema = schema
	return nil
}

func loadClassification(classificationPath string) (*gabs.Container, error) {
	classification, err := gabs.ParseJSONFile(classificationPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse classification file")
	}
	return classification, nil
}

// LoadImportance wiull load the importance feature selection metric.
func LoadImportance(m *model.Metadata, importanceFile string) error {
	// unmarshall the schema file
	importance, err := gabs.ParseJSONFile(importanceFile)
	if err != nil {
		return errors.Wrap(err, "failed to parse importance file")
	}
	// if no numeric fields, features will be null
	// NOTE: Assume all variables in a single resource since that is
	// how we would submit to ranking.
	if importance.Path("features").Data() != nil {
		metric := importance.Path("features").Children()
		if metric == nil {
			return errors.New("features attribute missing from file")
		}
		for index, v := range m.DataResources[0].Variables {
			// geocoded variables added after ranking on ingest
			if index < len(metric) {
				v.Importance = int(metric[index].Data().(float64)) + 1
			}
		}
	}
	return nil
}

func writeSummaryFile(summaryFile string, summary string) error {
	return ioutil.WriteFile(summaryFile, []byte(summary), 0644)
}

// LoadSummaryFromDescription loads a summary from the description.
func LoadSummaryFromDescription(m *model.Metadata, summaryFile string) error {
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
func LoadSummary(m *model.Metadata, summaryFile string, useCache bool) error {
	// use cache if available
	if useCache {
		b, err := ioutil.ReadFile(summaryFile)
		if err == nil {
			m.Summary = string(b)
			return nil
		}
	}
	return LoadSummaryFromDescription(m, summaryFile)
}

// LoadSummaryMachine loads a machine-learned summary.
func LoadSummaryMachine(m *model.Metadata, summaryFile string) error {
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
func LoadDatasetStats(m *model.Metadata, datasetPath string) error {

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

func loadID(m *model.Metadata) error {
	id, ok := m.Schema.Path("about.datasetID").Data().(string)
	if !ok {
		return errors.Errorf("no `about.datasetID` key found in schema")
	}
	m.ID = id
	return nil
}

func loadName(m *model.Metadata) error {
	name, ok := m.Schema.Path("about.datasetName").Data().(string)
	if !ok {
		return nil //errors.Errorf("no `name` key found in schema")
	}
	m.Name = name
	return nil
}

func loadAbout(m *model.Metadata) error {
	if m.Schema.Path("about.description").Data() != nil {
		m.Description = m.Schema.Path("about.description").Data().(string)
	}

	// default to using the normalized id as storage name
	if m.Schema.Path("about.storageName").Data() != nil {
		m.StorageName = m.Schema.Path("about.storageName").Data().(string)
	} else {
		m.StorageName = model.NormalizeDatasetID(m.Schema.Path("about.datasetID").Data().(string))
	}

	if m.Schema.Path("about.redacted").Data() != nil {
		m.Redacted = m.Schema.Path("about.redacted").Data().(bool)
	}

	return nil
}

func parseSchemaVariable(v *gabs.Container, existingVariables []*model.Variable, normalizeName bool) (*model.Variable, error) {
	if v.Path("colName").Data() == nil {
		return nil, fmt.Errorf("unable to parse column name")
	}
	varName := v.Path("colName").Data().(string)

	varDisplayName := ""
	if v.Path("colDisplayName").Data() != nil {
		varDisplayName = v.Path("colDisplayName").Data().(string)
	}

	varType := ""
	if v.Path("colType").Data() != nil {
		varType = v.Path("colType").Data().(string)
		varType = model.MapLLType(varType)
	}

	varDescription := ""
	if v.Path("colDescription").Data() != nil {
		varDescription = v.Path("colDescription").Data().(string)
	}

	varIndex := 0
	if v.Path("colIndex").Data() != nil {
		varIndex = int(v.Path("colIndex").Data().(float64))
	}

	var varRoles []string
	if v.Path("role").Data() != nil {
		rolesRaw := v.Path("role").Children()
		if rolesRaw == nil {
			return nil, errors.New("unable to parse column role")
		}
		varRoles = make([]string, len(rolesRaw))
		for i, r := range rolesRaw {
			varRoles[i] = r.Data().(string)
		}
	}

	varDistilRole := ""
	if v.Path("distilRole").Data() != nil {
		varDistilRole = v.Path("distilRole").Data().(string)
	}

	varOriginalName := ""
	if v.Path("varOriginalName").Data() != nil {
		varOriginalName = v.Path("varOriginalName").Data().(string)
	}

	// parse the refersTo fields to properly serialize it if necessary
	var refersTo map[string]interface{}
	if v.Path("refersTo").Data() != nil {
		refersTo = make(map[string]interface{})
		refersToData := v.Path("refersTo")
		resID := ""
		resObject := make(map[string]interface{})

		if refersToData.Path("resID").Data() != nil {
			resID = refersToData.Path("resID").Data().(string)
		}

		if refersToData.Path("resObject").Data() != nil {
			resObjectMap := refersToData.Path("resObject").ChildrenMap()
			if resObjectMap == nil {
				// see if it is maybe a string and if it is, ignore
				data, ok := refersToData.Path("resObject").Data().(string)
				if !ok {
					return nil, errors.New("unable to parse resObject")
				}
				refersTo["resObject"] = data
			} else {
				for k, v := range resObjectMap {
					resObject[k] = v.Data().(string)
				}
				refersTo["resObject"] = resObject
			}
		}

		refersTo["resID"] = resID
	}
	variable := model.NewVariable(
		varIndex,
		varName,
		varDisplayName,
		varOriginalName,
		varType,
		varType,
		varDescription,
		varRoles,
		varDistilRole,
		refersTo,
		existingVariables,
		normalizeName)
	variable.SuggestedTypes = append(variable.SuggestedTypes, &model.SuggestedType{
		Type:        variable.Type,
		Probability: 2,
		Provenance:  ProvenanceSchema,
	})

	return variable, nil
}

func cleanVarType(m *model.Metadata, name string, typ string) string {
	// set the d3m index to int regardless of what gets returned
	if name == model.D3MIndexName {
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

func parseClassification(m *model.Metadata, index int, labels []*gabs.Container) (string, error) {
	// parse classification
	col := labels[index]
	varTypeLabels := col.Children()
	if varTypeLabels == nil {
		return "", errors.Errorf("failed to parse classification for column `%d`", col)
	}
	if len(varTypeLabels) > 0 {
		// TODO: fix so we don't always just use first classification
		return varTypeLabels[0].Data().(string), nil
	}
	return model.DefaultVarType, nil
}

func parseSuggestedTypes(m *model.Metadata, name string, index int, labels []*gabs.Container, probabilities []*gabs.Container) ([]*model.SuggestedType, error) {
	// variables added after classification will not have suggested types
	if index >= len(labels) {
		return nil, nil
	}

	// parse probabilities
	labelsCol := labels[index]
	probabilitiesCol := probabilities[index]
	varTypeLabels := labelsCol.Children()
	if varTypeLabels == nil {
		return nil, errors.Errorf("failed to parse classification for column `%d`", labelsCol)
	}
	varProbabilities := probabilitiesCol.Children()
	if varProbabilities == nil {
		return nil, errors.Errorf("failed to parse probabilities for column `%d`", probabilitiesCol)
	}
	var suggested []*model.SuggestedType
	for index, label := range varTypeLabels {
		prob := varProbabilities[index]
		typ := label.Data().(string)
		probability := prob.Data().(float64)

		// adjust the probability for complex suggested types
		if !model.IsBasicSimonType(typ) {
			probability = probability * 1.5
		}

		suggested = append(suggested, &model.SuggestedType{
			Type:        cleanVarType(m, name, typ),
			Probability: probability,
			Provenance:  ProvenanceSimon,
		})
	}
	// sort by probability
	sort.Slice(suggested, func(i, j int) bool {
		return suggested[i].Probability > suggested[j].Probability
	})
	return suggested, nil
}

func loadOriginalSchemaVariables(m *model.Metadata, schemaPath string) error {
	dataResources := m.Schema.Path("dataResources").Children()
	if dataResources == nil {
		return errors.New("failed to parse data resources")
	}

	// Parse the variables for every schema
	m.DataResources = make([]*model.DataResource, len(dataResources))
	for i, sv := range dataResources {
		if sv.Path("resType").Data() == nil {
			return fmt.Errorf("unable to parse resource type")
		}
		resType := sv.Path("resType").Data().(string)

		var parser DataResourceParser
		switch resType {
		case model.ResTypeAudio, model.ResTypeImage, model.ResTypeText:
			parser = NewMedia(resType)
		case model.ResTypeTable:
			parser = &Table{}
		case model.ResTypeTime:
			parser = &Timeseries{}
		case model.ResTypeRaw:
			parser = &Raw{
				rootPath: path.Dir(schemaPath),
			}
		default:
			return errors.Errorf("Unrecognized resource type '%s'", resType)
		}

		dr, err := parser.Parse(sv)
		if err != nil {
			return errors.Wrapf(err, "Unable to parse data resource of type '%s'", resType)
		}

		m.DataResources[i] = dr
	}
	return nil
}

func loadMergedSchemaVariables(m *model.Metadata) error {
	schemaResources := m.Schema.Path("dataResources").Children()
	if schemaResources == nil {
		return errors.New("failed to parse merged resource data")
	}

	schemaVariables := schemaResources[0].Path("columns").Children()
	if schemaVariables == nil {
		return errors.New("failed to parse merged variable data")
	}

	// Merged schema has only one set of variables
	m.DataResources = make([]*model.DataResource, 1)
	m.DataResources[0] = &model.DataResource{
		Variables: make([]*model.Variable, 0),
	}

	for _, v := range schemaVariables {
		variable, err := parseSchemaVariable(v, m.DataResources[0].Variables, true)
		if err != nil {
			return errors.Wrap(err, "failed to parse merged schema variable")
		}
		m.DataResources[0].Variables = append(m.DataResources[0].Variables, variable)
	}
	return nil
}

func loadClassificationVariables(m *model.Metadata, normalizeVariableNames bool) error {
	schemaResources := m.Schema.Path("dataResources").Children()
	if schemaResources == nil {
		return errors.New("failed to parse merged resource data")
	}

	schemaVariables := schemaResources[0].Path("columns").Children()
	if schemaVariables == nil {
		return errors.New("failed to parse merged variable data")
	}

	labels := m.Classification.Path("labels").Children()
	if labels == nil {
		return errors.New("failed to parse classification labels")
	}

	probabilities := m.Classification.Path("label_probabilities").Children()
	if probabilities == nil {
		return errors.New("Unable to parse classification probabilities")
	}

	resPath := schemaResources[0].Path("resPath").Data().(string)

	// All variables now in a single dataset since it is merged
	m.DataResources = make([]*model.DataResource, 1)
	m.DataResources[0] = &model.DataResource{
		Variables: make([]*model.Variable, 0),
		ResPath:   resPath,
	}

	for index, v := range schemaVariables {
		variable, err := parseSchemaVariable(v, m.DataResources[0].Variables, normalizeVariableNames)
		if err != nil {
			return err
		}
		// get suggested types
		suggestedTypes, err := parseSuggestedTypes(m, variable.Name, index, labels, probabilities)
		if err != nil {
			return err
		}
		variable.SuggestedTypes = append(variable.SuggestedTypes, suggestedTypes...)
		// set type to that with highest probability
		if len(variable.SuggestedTypes) > 0 && variable.SuggestedTypes[0].Probability >= typeProbabilityThreshold {
			variable.Type = variable.SuggestedTypes[0].Type
		} else {
			variable.Type = model.DefaultVarType
		}
		m.DataResources[0].Variables = append(m.DataResources[0].Variables, variable)
	}
	return nil
}

func mergeVariables(m *model.Metadata, left []*gabs.Container, right []*gabs.Container) []*gabs.Container {
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
func WriteMergedSchema(m *model.Metadata, path string, mergedDataResource *model.DataResource) error {
	// create output format
	output := map[string]interface{}{
		"about": map[string]interface{}{
			"datasetID":            m.ID,
			"datasetName":          m.Name,
			"parentDatasetIDs":     m.ParentDatasetIDs,
			"storageName":          m.StorageName,
			"description":          m.Description,
			"datasetSchemaVersion": schemaVersion,
			"license":              license,
			"rawData":              m.Raw,
			"redacted":             m.Redacted,
			"mergedSchema":         "true",
		},
		"dataResources": []*model.DataResource{mergedDataResource},
	}
	bytes, err := json.MarshalIndent(output, "", "	")
	if err != nil {
		return errors.Wrap(err, "failed to marshal merged schema file output")
	}
	// write copy to disk
	return ioutil.WriteFile(path, bytes, 0644)
}

// WriteSchema exports the current meta data as a schema file.
func WriteSchema(m *model.Metadata, path string) error {
	dataResources := make([]interface{}, 0)
	for _, dr := range m.DataResources {
		dataResources = append(dataResources, dr)
	}

	output := map[string]interface{}{
		"about": map[string]interface{}{
			"datasetID":            m.ID,
			"datasetName":          m.Name,
			"parentDatasetIDs":     m.ParentDatasetIDs,
			"storageName":          m.StorageName,
			"description":          m.Description,
			"datasetSchemaVersion": schemaVersion,
			"license":              license,
			"rawData":              m.Raw,
			"redacted":             m.Redacted,
			"mergedSchema":         "false",
		},
		"dataResources": dataResources,
	}

	bytes, err := json.MarshalIndent(output, "", "	")
	if err != nil {
		return errors.Wrap(err, "failed to marshal merged schema file output")
	}
	// write copy to disk
	return ioutil.WriteFile(path, bytes, 0644)
}

// IngestMetadata adds a document consisting of the metadata to the
// provided index.
func IngestMetadata(client *elastic.Client, index string, datasetPrefix string, datasetSource DatasetSource, meta *model.Metadata) error {
	// filter variables for surce object
	if len(meta.DataResources) > 1 {
		return errors.New("metadata variables not merged into a single dataset")
	}

	// clear refers to
	for _, v := range meta.DataResources[0].Variables {
		v.RefersTo = nil
	}
	var origins []map[string]interface{}
	if meta.DatasetOrigins != nil {
		origins = make([]map[string]interface{}, len(meta.DatasetOrigins))
		for i, ds := range meta.DatasetOrigins {
			origins[i] = map[string]interface{}{
				"searchResult":  ds.SearchResult,
				"provenance":    ds.Provenance,
				"sourceDataset": ds.SourceDataset,
			}
		}
	}

	source := map[string]interface{}{
		"datasetName":      meta.Name,
		"datasetID":        meta.ID,
		"parentDatasetIDs": meta.ParentDatasetIDs,
		"storageName":      meta.StorageName,
		"description":      meta.Description,
		"summary":          meta.Summary,
		"summaryMachine":   meta.SummaryMachine,
		"numRows":          meta.NumRows,
		"numBytes":         meta.NumBytes,
		"variables":        meta.DataResources[0].Variables,
		"datasetFolder":    meta.DatasetFolder,
		"source":           datasetSource,
		"datasetOrigins":   origins,
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
		Refresh("true").
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to add document to index `%s`", index)
	}
	return nil
}

// DatasetMatches determines if the metadata variables match.
func DatasetMatches(m *model.Metadata, variables []string) bool {
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
			deleted, err := client.
				DeleteIndex(index).
				Do(context.Background())
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
					},
					"search_filter": {
						"type": "edge_ngram",
						"min_gram": 1,
						"max_gram": 20
					}
				},
				"tokenizer": {
					"search_tokenizer": {
						"type": "edge_ngram",
						"min_gram": 1,
						"max_gram": 20,
						"token_chars": [
							"letter",
							"digit"
						]
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
					},
					"search_analyzer": {
						"type": "custom",
						"tokenizer": "search_tokenizer",
						"filter": [
							"lowercase",
							"search_filter"
						]
					},
					"id_analyzer": {
						"type":	  "pattern",
						"pattern":   "\\W|_",
						"lowercase": true
					}
				}
			}
		},
		"mappings": {
			"metadata": {
				"properties": {
					"datasetID": {
						"type": "text",
						"analyzer": "search_analyzer"
					},
					"datasetName": {
						"type": "text",
						"analyzer": "search_analyzer",
						"fields": {
							"keyword": {
								"type": "keyword",
								"ignore_above": 256
							}
						}
					},
					"parentDatasetIDs": {
						"type": "text",
						"analyzer": "search_analyzer"
					},
					"storageName": {
						"type": "text"
					},
					"datasetFolder": {
						"type": "text"
					},
					"description": {
						"type": "text",
						"analyzer": "search_analyzer"
					},
					"summary": {
						"type": "text",
						"analyzer": "search_analyzer"
					},
					"summaryMachine": {
						"type": "text",
						"analyzer": "search_analyzer"
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
								"analyzer": "search_analyzer",
								"include_in_all": true,
								"term_vector": "yes"
							},
							"colName": {
								"type": "text",
								"analyzer": "search_analyzer",
								"include_in_all": true,
								"term_vector": "yes"
							},
							"varRole": {
								"type": "text"
							},
							"varType": {
								"type": "text"
							},
							"varOriginalType": {
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
	created, err := client.
		CreateIndex(index).
		BodyString(body).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "failed to create index %s", index)
	}
	if !created.Acknowledged {
		return fmt.Errorf("Failed to create new index %s", index)
	}
	return nil
}
