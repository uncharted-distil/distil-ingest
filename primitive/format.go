package primitive

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-ingest/metadata"
	"github.com/uncharted-distil/distil-ingest/util"
)

// Format will format a dataset to have the required structures for D3M.
func (s *IngestStep) Format(schemaFile string, dataset string,
	rootDataPath string, outputFolder string, hasHeader bool) error {
	meta, err := metadata.LoadMetadataFromOriginalSchema(schemaFile)
	if err != nil {
		return errors.Wrap(err, "unable to load original schema file")
	}

	// fix for d3m index requirement
	if !checkD3MIndexExists(meta) {
		err = s.addD3MIndex(schemaFile, dataset, meta, outputFolder, hasHeader)
		if err != nil {
			return errors.Wrap(err, "unable to add d3m index")
		}
	} else {
		// copy to output for standard structure going forward
		os.MkdirAll(outputFolder, os.ModePerm)
		err := copy.Copy(path.Dir(dataset), outputFolder)
		if err != nil {
			return errors.Wrap(err, "unable to copy source data")
		}
	}

	return nil
}

func (s *IngestStep) addD3MIndex(schemaFile string, dataset string, meta *model.Metadata, outputFolder string, hasHeader bool) error {
	// check to make sure only a single data resource exists
	if len(meta.DataResources) != 1 {
		return errors.Errorf("adding d3m index requires that the dataset have only 1 data resource (%d exist)", len(meta.DataResources))
	}

	// copy the data to a new directory
	outputSchemaPath := path.Join(outputFolder, D3MSchemaPathRelative)
	outputDataPath := path.Join(outputFolder, D3MDataPathRelative)
	sourceFolder := path.Dir(dataset)

	// copy the source folder to have all the linked files for merging
	os.MkdirAll(outputFolder, os.ModePerm)
	err := copy.Copy(sourceFolder, outputFolder)
	if err != nil {
		return errors.Wrap(err, "unable to copy source data")
	}

	// delete the existing files that will be overwritten
	os.Remove(outputSchemaPath)
	os.Remove(outputDataPath)

	// add the d3m index variable to the metadata
	dr := meta.DataResources[0]
	name := model.D3MIndexFieldName
	v := model.NewVariable(len(dr.Variables), name, name, name, model.IntegerType, model.IntegerType, []string{"index"}, model.VarRoleIndex, nil, dr.Variables, false)
	dr.Variables = append(dr.Variables, v)

	// read the raw data
	dataPath := path.Join(path.Dir(schemaFile), dr.ResPath)
	lines, err := s.readCSVFile(dataPath, hasHeader)
	if err != nil {
		return errors.Wrap(err, "error reading raw data")
	}

	// append the row count as d3m index
	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output the header
	header := make([]string, len(dr.Variables))
	for _, v := range dr.Variables {
		header[v.Index] = v.Name
	}
	err = writer.Write(header)
	if err != nil {
		return errors.Wrap(err, "error storing format header")
	}

	// parse the raw output and write the line out
	for i, line := range lines {
		line = append(line, fmt.Sprintf("%d", i+1))

		err = writer.Write(line)
		if err != nil {
			return errors.Wrap(err, "error storing feature output")
		}
	}

	// output the data with the new feature
	writer.Flush()
	err = util.WriteFileWithDirs(outputDataPath, output.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error writing feature output")
	}

	relativePath := getRelativePath(path.Dir(outputSchemaPath), outputDataPath)
	dr.ResPath = relativePath
	dr.ResType = model.ResTypeTable

	// write the new schema to file
	err = metadata.WriteSchema(meta, outputSchemaPath)
	if err != nil {
		return errors.Wrap(err, "unable to store feature schema")
	}

	return nil
}

func checkD3MIndexExists(meta *model.Metadata) bool {
	// check all variables for a d3m index
	for _, dr := range meta.DataResources {
		for _, v := range dr.Variables {
			if v.Name == model.D3MIndexFieldName {
				return true
			}
		}
	}

	return false
}
