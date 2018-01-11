package rest

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// ClassificationResult represents a REST classification result.
type ClassificationResult struct {
	Labels        [][]string  `json:"labels"`
	Probabilities [][]float64 `json:"label_probabilities"`
	Path          string      `json:"path"`
}

// Classifier is user to classify data types.
type Classifier struct {
	functionName string
	client       *Client
}

// NewClassifier creates a classifier using the specified client.
func NewClassifier(functionName string, client *Client) *Classifier {
	return &Classifier{
		functionName: functionName,
		client:       client,
	}
}

// ClassifyFile classifies the data types found in a file that follows the
// usual schema structure.
func (c *Classifier) ClassifyFile(filename string) (*ClassificationResult, error) {
	result, err := c.client.PostFile(c.functionName, filename, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to classify file")
	}

	// Structure of json is pretty generic:
	// [0] = types
	// [1] = probabilities
	classifiedData := make([]interface{}, 0)
	err = json.Unmarshal(result, &classifiedData)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal classification response")
	}

	// Types are [][]string. Need to parse everything manually.
	typesRaw, ok := classifiedData[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("Unable to parse returned types")
	}
	types := make([][]string, len(typesRaw))
	for i, tr := range typesRaw {
		t, ok := tr.([]interface{})
		if !ok {
			return nil, fmt.Errorf("Unable to parse returned type strings")
		}
		types[i] = make([]string, len(t))

		// Parse the inner string array.
		for j, ts := range t {
			s, ok := ts.(string)
			if !ok {
				return nil, fmt.Errorf("Unable to parse returned type string")
			}
			types[i][j] = s
		}
	}

	// Probabilities are [][]float64. Need to parse it all manually.
	probabilitiesRaw, ok := classifiedData[1].([]interface{})
	if !ok {
		return nil, fmt.Errorf("Unable to parse returned probabilities")
	}
	probabilities := make([][]float64, len(probabilitiesRaw))
	for i, pr := range probabilitiesRaw {
		p, ok := pr.([]interface{})
		if !ok {
			return nil, fmt.Errorf("Unable to parse returned probability floats")
		}
		probabilities[i] = make([]float64, len(p))

		// Parse the inner float array.
		for j, pf := range p {
			f, ok := pf.(float64)
			if !ok {
				return nil, fmt.Errorf("Unable to parse returned probability float")
			}
			probabilities[i][j] = f
		}
	}

	return &ClassificationResult{
		Labels:        types,
		Probabilities: probabilities,
		Path:          filename,
	}, nil
}
