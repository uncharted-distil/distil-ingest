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

package metadata

import (
	"encoding/csv"
	"io"
	"os"

	"github.com/araddon/dateparse"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
)

// VerifyAndUpdate will update the metadata when inconsistentices or errors
// are found.
func VerifyAndUpdate(m *model.Metadata, dataPath string) error {
	// read the data
	csvFile, err := os.Open(dataPath)
	if err != nil {
		return errors.Wrap(err, "failed to open data file")
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)

	// skip header
	_, err = reader.Read()
	if err != nil {
		return errors.Wrap(err, "failed to read header from data file")
	}

	// cycle through the whole dataset
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "failed to read line from data file")
		}

		err = checkTypes(m, line)
		if err != nil {
			return errors.Wrap(err, "unable to check data types")
		}
	}

	return nil
}

func checkTypes(m *model.Metadata, row []string) error {
	// cycle through all variables
	for _, v := range m.DataResources[0].Variables {
		// set the type to text if the data doesn't match the metadata
		if !typeMatchesData(v, row) {
			v.Type = model.TextType
		}
	}

	return nil
}

func typeMatchesData(v *model.Variable, row []string) bool {
	val := row[v.Index]
	good := true

	switch v.Type {
	case model.DateTimeType:
		_, err := dateparse.ParseAny(val)
		good = err == nil
		break
	}

	return good
}
