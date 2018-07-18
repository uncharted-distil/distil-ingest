package rest

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	minClusterCount = 5
)

// ImageResult represents a REST image feature result.
type ImageResult struct {
	Image map[string]interface{} `json:"image"`
}

// Featurizer is used to featurize images files.
type Featurizer struct {
	functionName string
	client       *Client
}

// NewFeaturizer creates a featurizer using the specified client.
func NewFeaturizer(functionName string, client *Client) *Featurizer {
	return &Featurizer{
		functionName: functionName,
		client:       client,
	}
}

// FeaturizeImage produces features from an image file.
func (f *Featurizer) FeaturizeImage(filename string) (*ImageResult, error) {
	params := map[string]string{
		"image_path": filename,
	}
	result, err := f.client.PostRequest(f.functionName, params)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to featurize file")
	}

	// response is a json of objects and text found in the image
	imageData := make(map[string]interface{}, 0)
	err = json.Unmarshal(result, &imageData)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal image response")
	}
	return &ImageResult{
		Image: imageData,
	}, nil
}

// ClusterImages places images into similar clusters.
func (f *Featurizer) ClusterImages(filenames []string) (*ImageResult, error) {
	if len(filenames) < minClusterCount {
		imageClusters := make(map[string]interface{})
		for i := range filenames {
			imageClusters[fmt.Sprintf("%d", i)] = i
		}
		images := &ImageResult{
			Image: make(map[string]interface{}),
		}
		images.Image["pred_class"] = imageClusters
		return images, nil
	}

	params := map[string]interface{}{
		"image_paths": filenames,
	}
	result, err := f.client.PostRequestRaw(f.functionName, params)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to cluster file")
	}

	// response is a json of objects and text found in the image
	imageData := make(map[string]interface{}, 0)
	err = json.Unmarshal(result, &imageData)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to unmarshal image response")
	}
	return &ImageResult{
		Image: imageData,
	}, nil
}
