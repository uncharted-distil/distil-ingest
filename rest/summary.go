package rest

import (
	"strings"

	"github.com/pkg/errors"
)

// ClassificationResult represents a REST classification result.
type SummaryResult struct {
	Summary string `json:"summary"`
}

// Classifier is user to classify data types.
type Summarizer struct {
	functionName string
	client       *Client
}

// NewClassifier creates a classifier using the specified client.
func NewSummarizer(functionName string, client *Client) *Summarizer {
	return &Summarizer{
		functionName: functionName,
		client:       client,
	}
}

// ClassifyFile classifies the data types found in a file that follows the
// usual schema structure.
func (s *Summarizer) SummarizeFile(filename string) (*SummaryResult, error) {
	result, err := s.client.PostFile(s.functionName, filename, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to classify file")
	}

	// Structure of response is a string describing the dataset
	summary := string(result)
	summary = strings.Replace(summary, "\"", "", -1)
	return &SummaryResult{
		Summary: summary,
	}, nil
}
