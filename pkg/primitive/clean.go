//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package primitive

import (
	"bytes"
	"encoding/csv"
	"os"
	"path"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-compute/primitive/compute/description"
	"github.com/uncharted-distil/distil-ingest/pkg/util"
)

// Clean will clean bad data for further processing.
func (s *IngestStep) Clean(dataset string, outputFolder string) error {
	outputSchemaPath := path.Join(outputFolder, D3MSchemaPathRelative)
	outputDataPath := path.Join(outputFolder, D3MDataPathRelative)
	sourceFolder := path.Dir(dataset)

	// copy the source folder to have all the linked files for merging
	err := os.MkdirAll(outputFolder, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to make output folder")
	}

	err = copy.Copy(sourceFolder, outputFolder)
	if err != nil {
		return errors.Wrap(err, "unable to copy source data")
	}

	// delete the existing files that will be overwritten
	os.Remove(outputSchemaPath)
	os.Remove(outputDataPath)

	// load metadata from original schema
	meta, err := metadata.LoadMetadataFromOriginalSchema(dataset)
	if err != nil {
		return errors.Wrap(err, "unable to load original schema file")
	}
	mainDR := meta.GetMainDataResource()

	// create & submit the solution request
	pip, err := description.CreateDataCleaningPipeline("Mary Poppins", "")
	if err != nil {
		return errors.Wrap(err, "unable to create cleaning pipeline")
	}

	// pipeline execution assumes datasetDoc.json as schema file
	datasetURI, err := s.submitPrimitive([]string{sourceFolder}, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run cleaning pipeline")
	}

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output the header
	header := make([]string, len(mainDR.Variables))
	for _, v := range mainDR.Variables {
		header[v.Index] = v.Name
	}
	err = writer.Write(header)
	if err != nil {
		return errors.Wrap(err, "error storing clean header")
	}

	// parse primitive response (raw data from the input dataset)
	// first row of the data is the header
	// first column of the data is the dataframe index
	csvData, err := s.readCSVFile(datasetURI, true)
	if err != nil {
		return errors.Wrap(err, "unable to parse clean result")
	}

	// output the data
	for _, res := range csvData {
		err = writer.Write(res)
		if err != nil {
			return errors.Wrap(err, "error storing clean data")
		}
	}

	// output the data with the new feature
	writer.Flush()

	err = util.WriteFileWithDirs(outputDataPath, output.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error writing clustered output")
	}

	relativePath := getRelativePath(path.Dir(outputSchemaPath), outputDataPath)
	mainDR.ResPath = relativePath

	// write the new schema to file
	err = metadata.WriteSchema(meta, outputSchemaPath, true)
	if err != nil {
		return errors.Wrap(err, "unable to store cluster schema")
	}

	return nil
}
