package primitive

import (
	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/distil-ingest/primitive/compute/description"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/result"
)

// RankPrimitive will rank the dataset using a primitive.
func (s *IngestStep) MergePrimitive(dataset string, outputPath string) error {
	// create & submit the solution request
	pip, err := description.CreateDenormalizePipeline("3NF", "")
	if err != nil {
		return errors.Wrap(err, "unable to create denormalize pipeline")
	}

	datasetURI, err := s.submitPrimitive(dataset, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run PCA pipeline")
	}

	// parse primitive response (col index,importance)
	log.Infof("MERGING: %v", datasetURI)
	_, err = result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse PCA pipeline result")
	}

	return nil
}
