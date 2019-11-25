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
	"encoding/json"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/uncharted-distil/distil-ingest/rest"

	"github.com/uncharted-distil/distil-compute/primitive/compute/description"
	"github.com/uncharted-distil/distil-compute/primitive/compute/result"
	"github.com/uncharted-distil/distil-ingest/util"
)

// Rank will rank the dataset using a primitive.
func (s *IngestStep) Rank(dataset string, outputPath string) error {
	// create & submit the solution request
	pip, err := description.CreatePCAFeaturesPipeline("harry", "")
	if err != nil {
		return errors.Wrap(err, "unable to create PCA pipeline")
	}

	datasetURI, err := s.submitPrimitive([]string{dataset}, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run PCA pipeline")
	}

	// parse primitive response (col index,importance)
	res, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse PCA pipeline result")
	}

	ranks := make([]float64, len(res)-1)
	for i, v := range res {
		if i > 0 {
			colIndex, err := strconv.ParseInt(v[0].(string), 10, 64)
			if err != nil {
				return errors.Wrap(err, "unable to parse PCA col index")
			}
			vInt, err := strconv.ParseFloat(v[1].(string), 64)
			if err != nil {
				return errors.Wrap(err, "unable to parse PCA rank value")
			}
			ranks[colIndex] = vInt
		}
	}

	importance := &rest.ImportanceResult{
		Path:     datasetURI,
		Features: ranks,
	}

	// output the classification in the expected JSON format
	bytes, err := json.MarshalIndent(importance, "", "    ")
	if err != nil {
		return errors.Wrap(err, "unable to serialize ranking result")
	}

	// write to file
	err = util.WriteFileWithDirs(outputPath, bytes, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to store ranking result")
	}

	return nil
}
