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

	"github.com/pkg/errors"
	"github.com/uncharted-distil/distil-ingest/rest"

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-compute/primitive/compute/description"
	"github.com/uncharted-distil/distil-compute/primitive/compute/result"
	"github.com/uncharted-distil/distil-ingest/util"
)

// Classify will classify the dataset using a primitive.
func (s *IngestStep) Classify(dataset string, outputPath string) error {
	// create & submit the solution request
	pip, err := description.CreateSimonPipeline("says", "")
	if err != nil {
		return errors.Wrap(err, "unable to create Simon pipeline")
	}

	datasetURI, err := s.submitPrimitive([]string{dataset}, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run Simon pipeline")
	}

	// parse primitive response (variable,probabilities,labels)
	res, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse Simon pipeline result")
	}

	// First row is header, then all other rows are types, probabilities.
	probabilities := make([][]float64, len(res)-1)
	labels := make([][]string, len(res)-1)
	for i, v := range res {
		if i > 0 {
			colIndex := i - 1
			fieldLabels := toStringArray(v[0].([]interface{}))
			labels[colIndex] = mapClassifiedTypes(fieldLabels)
			probs, err := toFloat64Array(v[1].([]interface{}))
			if err != nil {
				return err
			}
			probabilities[colIndex] = probs
		}
	}
	classification := &rest.ClassificationResult{
		Path:          datasetURI,
		Labels:        labels,
		Probabilities: probabilities,
	}

	// output the classification in the expected JSON format
	bytes, err := json.MarshalIndent(classification, "", "    ")
	if err != nil {
		return errors.Wrap(err, "unable to serialize classification result")
	}
	// write to file
	err = util.WriteFileWithDirs(outputPath, bytes, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to store classification result")
	}

	return nil
}

func mapClassifiedTypes(types []string) []string {
	for i, typ := range types {
		types[i] = model.MapSimonType(typ)
	}

	return types
}
