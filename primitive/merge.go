package primitive

import (
	"bytes"
	"encoding/csv"
	"os"
	"path"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/distil-compute/model"
	"github.com/unchartedsoftware/distil-compute/pipeline"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/description"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/result"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// Merge will merge data resources into a single data resource.
func (s *IngestStep) Merge(dataset string, outputFolder string) error {
	outputSchemaPath := path.Join(outputFolder, D3MSchemaPathRelative)
	outputDataPath := path.Join(outputFolder, D3MDataPathRelative)
	sourceFolder := path.Dir(dataset)

	// copy the source folder to have all the linked files for merging
	err := copy.Copy(sourceFolder, outputFolder)
	if err != nil {
		return errors.Wrap(err, "unable to copy source data")
	}

	// delete the existing files that will be overwritten
	os.Remove(outputSchemaPath)
	os.Remove(outputDataPath)

	// need to manually build the metadata and output it.
	meta, err := metadata.LoadMetadataFromOriginalSchema(dataset)
	if err != nil {
		return errors.Wrap(err, "unable to load original metadata")
	}

	// create & submit the solution request
	var pip *pipeline.PipelineDescription
	timeseries, mainResID, refIndex := isTimeseriesDataset(meta)
	if timeseries {
		pip, err = description.CreateTimeseriesFormatterPipeline("Time Cop", "", mainResID, refIndex)
		if err != nil {
			return errors.Wrap(err, "unable to create denormalize pipeline")
		}
	} else {
		pip, err = description.CreateDenormalizePipeline("3NF", "")
		if err != nil {
			return errors.Wrap(err, "unable to create denormalize pipeline")
		}
	}

	// pipeline execution assumes datasetDoc.json as schema file
	datasetURI, err := s.submitPrimitive([]string{sourceFolder}, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run denormalize pipeline")
	}

	// parse primitive response (raw data from the input dataset)
	rawResults, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse denormalize result")
	}
	mainDR := meta.GetMainDataResource()
	vars := s.mapFields(meta)
	varsDenorm := s.mapDenormFields(mainDR)
	for k, v := range varsDenorm {
		vars[k] = v
	}

	outputMeta := model.NewMetadata(meta.ID, meta.Name, meta.Description, meta.StorageName)
	outputMeta.DataResources = append(outputMeta.DataResources, model.NewDataResource("0", mainDR.ResType, mainDR.ResFormat))
	header := rawResults[0]
	for i, field := range header {
		// the first column is a row idnex and should be discarded.
		if i > 0 {
			fieldName, ok := field.(string)
			if !ok {
				return errors.Errorf("unable to cast field name")
			}

			v := vars[fieldName]
			if v == nil {
				// create new variables (ex: series_id)
				v = model.NewVariable(i, fieldName, fieldName, fieldName, model.TextType, model.TextType, []string{"attribute"}, model.VarRoleData, nil, outputMeta.DataResources[0].Variables, false)
			}
			v.Index = i - 1
			outputMeta.DataResources[0].Variables = append(outputMeta.DataResources[0].Variables, v)
		}
	}

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// returned header doesnt match expected header so use metadata header
	headerMetadata, err := outputMeta.GenerateHeaders()
	if err != nil {
		return errors.Wrapf(err, "unable to generate header")
	}
	writer.Write(headerMetadata[0])

	// rewrite the output without the first column
	rawResults = rawResults[1:]
	for _, line := range rawResults {
		lineString := make([]string, len(line)-1)
		for i := 1; i < len(line); i++ {
			lineString[i-1] = line[i].(string)
		}
		writer.Write(lineString)
	}

	// output the data
	writer.Flush()
	err = util.WriteFileWithDirs(outputDataPath, output.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error writing merged output")
	}

	relativePath := getRelativePath(path.Dir(outputSchemaPath), outputDataPath)
	outputMeta.DataResources[0].ResPath = relativePath

	// write the new schema to file
	err = metadata.WriteSchema(outputMeta, outputSchemaPath)
	if err != nil {
		return errors.Wrap(err, "unable to store merged schema")
	}

	return nil
}

func (s *IngestStep) mapFields(meta *model.Metadata) map[string]*model.Variable {
	// cycle through each data resource, mapping field names to variables.
	fields := make(map[string]*model.Variable)
	for _, dr := range meta.DataResources {
		for _, v := range dr.Variables {
			fields[v.Name] = v
		}
	}

	return fields
}

func (s *IngestStep) mapDenormFields(mainDR *model.DataResource) map[string]*model.Variable {
	fields := make(map[string]*model.Variable)
	for _, field := range mainDR.Variables {
		if field.IsMediaReference() {
			// DENORM PRIMITIVE RENAMES REFERENCE FIELDS TO `filename`
			fields[denormFieldName] = field
		}
	}
	return fields
}

func isTimeseriesDataset(meta *model.Metadata) (bool, string, int) {
	mainDR := meta.GetMainDataResource()

	// check references to see if any point to a time series
	for _, v := range mainDR.Variables {
		if v.RefersTo != nil {
			resID := v.RefersTo["resID"].(string)
			res := getResource(meta, resID)
			if res != nil && res.ResType == "timeseries" {
				return true, mainDR.ResID, v.Index
			}
		}
	}

	return false, "", -1
}

func getResource(meta *model.Metadata, resID string) *model.DataResource {
	for _, dr := range meta.DataResources {
		if dr.ResID == resID {
			return dr
		}
	}

	return nil
}
