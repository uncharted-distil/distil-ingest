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

	fmt.Printf("RANKED: %s\n", string(result))

	importanceData := make([]interface{}, 0)
	err = json.Unmarshal(result, &importanceData)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal importance response")
	}

	// importanceRaw, ok := result.([]interface{})
	// if !ok {
	// 	return nil, fmt.Errorf("Unable to parse returned types")
	// }
	// // Probabilities are [][]float64. Need to parse it all manually.
	// probabilitiesRaw, ok := classifiedData.([]interface{})
	// if !ok {
	// 	return nil, fmt.Errorf("Unable to parse returned probabilities")
	// }
	// probabilities := make([][]float64, len(probabilitiesRaw))
	// for i, pr := range probabilitiesRaw {
	// 	p, ok := pr.([]interface{})
	// 	if !ok {
	// 		return nil, fmt.Errorf("Unable to parse returned probability floats")
	// 	}
	// 	probabilities[i] = make([]float64, len(p))
	//
	// 	// Parse the inner float array.
	// 	for j, pf := range p {
	// 		f, ok := pf.(float64)
	// 		if !ok {
	// 			return nil, fmt.Errorf("Unable to parse returned probability float")
	// 		}
	// 		probabilities[i][j] = f
	// 	}
	// }

	return nil, nil
}
