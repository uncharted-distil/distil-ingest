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

package split

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
)

func isNumeric(typ string) bool {
	switch typ {
	case "integer", "float":
		return true
	default:
		return false
	}
}

// GetNumericColumnIndices returns a slice with the columsn for numeric types.
func GetNumericColumnIndices(meta *model.Metadata) []int {
	// NOTE: Assume that a merged schema is being processed
	// so all variables in a single data resource.
	var numericCols []int
	for index, variable := range meta.DataResources[0].Variables {
		if isNumeric(variable.Type) {
			numericCols = append(numericCols, index)
		}
	}
	return numericCols
}

// GetNumericColumns creates a new csv file of all numeric columns.
func GetNumericColumns(filename string, meta *model.Metadata, hasHeader bool) ([]byte, error) {

	// open the left and outfiles for line-by-line by processing
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}

	numericCols := GetNumericColumnIndices(meta)

	reader := csv.NewReader(file)

	// output writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)
	count := 0
	var prevLine []string
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "failed to read line from file")
		}
		if count > 0 || !hasHeader {
			numericLine := make([]string, len(numericCols))
			for index, colIndex := range numericCols {
				// TODO: this is a temp fix for missing values
				val := line[colIndex]
				if val == "" {
					if prevLine != nil && prevLine[colIndex] != "" {
						// substitute previous rows value if we have it
						val = prevLine[colIndex]
					} else {
						// otherwise 0
						val = "0"
					}
				}
				numericLine[index] = val
			}
			// write the csv line back out
			writer.Write(numericLine)
			prevLine = line
		}
		count++
	}
	// flush writer
	writer.Flush()

	// close left
	err = file.Close()
	if err != nil {
		return nil, errors.Wrap(err, "failed to close input file")
	}

	return output.Bytes(), nil
}
