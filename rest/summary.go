package rest

import (
	"strings"

	"github.com/pkg/errors"
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

	// Structure of response is a string describing the dataset
	summary := string(result)
	summary = strings.Replace(summary, "\"", "", -1)
	return &SummaryResult{
		Summary: summary,
	}, nil
}
