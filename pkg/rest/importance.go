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

package rest

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

//Ranker is used to rank variable importance.
type Ranker struct {
	functionName string
	client       *Client
}

// ImportanceResult is the result from a ranking operation.
type ImportanceResult struct {
	Path     string    `json:"path"`
	Features []float64 `json:"features"`
}

// NewRanker creates a ranker using the specified client.
func NewRanker(functionName string, client *Client) *Ranker {
	return &Ranker{
		functionName: functionName,
		client:       client,
	}
}

// RankFile ranks the importance of the variables in a file.
// Ranking can only be done on NUMERIC types.
func (r *Ranker) RankFile(filename string) (*ImportanceResult, error) {
	result, err := r.client.PostFile(r.functionName, filename, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to rank file")
	}

	return r.parseResult(filename, result)
}

// RankFileForTarget ranks the importance of the variables for a given target in a file.
func (r *Ranker) RankFileForTarget(filename string, targetName string) (*ImportanceResult, error) {
	result, err := r.client.PostFile(r.functionName, filename, map[string]string{"target": targetName})
	if err != nil {
		return nil, errors.Wrap(err, "Unable to rank file")
	}

	return r.parseResult(filename, result)
}

func (r *Ranker) parseResult(filename string, data []byte) (*ImportanceResult, error) {
	importanceData := make([]interface{}, 0)
	err := json.Unmarshal(data, &importanceData)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal importance response")
	}

	features := make([]float64, len(importanceData))
	for i, imp := range importanceData {
		f, ok := imp.(float64)
		if !ok {
			return nil, fmt.Errorf("Unable to parse float importance ranking")
		}
		features[i] = f
	}

	return &ImportanceResult{
		Path:     filename,
		Features: features,
	}, nil
}
