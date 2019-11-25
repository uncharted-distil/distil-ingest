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
	"strings"

	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-ingest/pkg/csv"
)

// SummaryResult represents a REST summary result.
type SummaryResult struct {
	Summary string `json:"summary"`
}

// Summarizer is user to summarize data files.
type Summarizer struct {
	functionName string
	client       *Client
}

// NewSummarizer creates a summarizer using the specified client.
func NewSummarizer(functionName string, client *Client) *Summarizer {
	return &Summarizer{
		functionName: functionName,
		client:       client,
	}
}

// SummarizeFile summarizes the data found in a file that follows the
// usual schema structure.
func (s *Summarizer) SummarizeFile(filename string) (*SummaryResult, error) {
	result, err := s.client.PostFile(s.functionName, filename, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to summarize file")
	}

	// Structure of response is an array of label + probability
	summaryRaw := string(result)
	summaryParsed := csv.ParseResultCSVString([]string{summaryRaw})[0]
	labelsRaw, ok := summaryParsed.([]interface{})
	if !ok {
		return nil, errors.Wrap(err, "Unable to parse outer summary result")
	}

	labelsRaw, ok = labelsRaw[0].([]interface{})
	if !ok {
		return nil, errors.Wrap(err, "Unable to parse raw summary result")
	}

	labels := make([]string, len(labelsRaw))
	for i, l := range labelsRaw {
		label, ok := l.(string)
		if !ok {
			return nil, errors.Wrap(err, "Unable to parse nested summary result")
		}

		labels[i] = label
	}

	return &SummaryResult{
		Summary: strings.Join(labels, ","),
	}, nil
}
