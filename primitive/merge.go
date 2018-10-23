package primitive

import (
	"bytes"
	"encoding/csv"
	"os"
	"path"

	"github.com/pkg/errors"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/description"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/result"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// RankPrimitive will rank the dataset using a primitive.
func (s *IngestStep) MergePrimitive(dataset string, outputSchemaPath string, outputDataPath string) error {
	// create & submit the solution request
	pip, err := description.CreateDenormalizePipeline("3NF", "")
	if err != nil {
		return errors.Wrap(err, "unable to create denormalize pipeline")
	}

	datasetURI, err := s.submitPrimitive(dataset, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run denormalize pipeline")
	}

	// parse primitive response (raw data from the input dataset)
	rawResults, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse denormalize result")
	}

	// need to manually build the metadata and output it.
	meta, err := metadata.LoadMetadataFromOriginalSchema(dataset)
	if err != nil {
		return errors.Wrap(err, "unable to load original metadata")
	}
	vars := s.mapFields(meta)

	outputMeta := metadata.NewMetadata()
	header := rawResults[0]
	for i, field := range header {
		// the first column is a row idnex and should be discarded.
		if i > 0 {
			fieldName, ok := field.(string)
			if !ok {
				return errors.Errorf("unable to cast field name")
			}

			v := vars[fieldName]
			v.Index = i - 1
			outputMeta.DataResources[0].Variables = append(outputMeta.DataResources[0].Variables, v)
		}
	}

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// rewrite the output without the first column
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
	err = meta.WriteSchema(outputSchemaPath)
	if err != nil {
		return errors.Wrap(err, "unable to store merged schema")
	}

	return nil
}

func (s *IngestStep) mapFields(meta *metadata.Metadata) map[string]*metadata.Variable {
	// cycle through each data resource, mapping field names to variables.
	fields := make(map[string]*metadata.Variable)
	for _, dr := range meta.DataResources {
		for _, v := range dr.Variables {
			fields[v.Name] = v
		}
	}

	return fields
}
