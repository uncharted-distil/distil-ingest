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
	result, err := r.client.PostFile(r.functionName, filename)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to rank file")
	}

	importanceData := make([]interface{}, 0)
	err = json.Unmarshal(result, &importanceData)
	if err != nil {
		fmt.Printf("RANKED: %v", string(result))
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
