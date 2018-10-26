package description

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/pipeline"
)

const defaultResource = "0"

// CreateUserDatasetPipeline creates a pipeline description to capture user feature selection and
// semantic type information.
func CreateUserDatasetPipeline(name string, description string, targetFeature string) (*pipeline.PipelineDescription, error) {

	// instantiate the pipeline
	builder := NewBuilder(name, description)

	pip, err := builder.AddInferencePoint().Compile()
	if err != nil {
		return nil, err
	}

	// Input set to arbitrary string for now
	pip.Inputs = []*pipeline.PipelineDescriptionInput{{
		Name: "dataset",
	}}
	return pip, nil
}

// CreateSlothPipeline creates a pipeline to peform timeseries clustering on a dataset.
func CreateSlothPipeline(name string, description string, targetColumn string, timeColumn string, valueColumn string,
	baseFeatures []*metadata.Variable, timeSeriesFeatures []*metadata.Variable) (*pipeline.PipelineDescription, error) {

	targetIdx, err := getIndex(baseFeatures, targetColumn)
	if err != nil {
		return nil, err
	}

	timeIdx, err := getIndex(timeSeriesFeatures, timeColumn)
	if err != nil {
		return nil, err
	}

	valueIdx, err := getIndex(timeSeriesFeatures, valueColumn)
	if err != nil {
		return nil, err
	}

	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDenormalizeStep()).
		Add(NewDatasetToDataframeStep()).
		Add(NewTimeSeriesLoaderStep(targetIdx, timeIdx, valueIdx)).
		Add(NewSlothStep()).
		Compile()

	if err != nil {
		return nil, err
	}

	return pipeline, nil
}

// CreateDukePipeline creates a pipeline to peform image featurization on a dataset.
func CreateDukePipeline(name string, description string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDatasetToDataframeStep()).
		Add(NewDukeStep()).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

// CreateSimonPipeline creates a pipeline to run semantic type inference on a dataset's
// columns.
func CreateSimonPipeline(name string, description string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDatasetToDataframeStep()).
		Add(NewSimonStep()).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

// CreateCrocPipeline creates a pipeline to run image featurization on a dataset.
func CreateCrocPipeline(name string, description string, targetColumns []string, outputLabels []string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDenormalizeStep()).
		Add(NewDatasetToDataframeStep()).
		Add(NewCrocStep(targetColumns, outputLabels)).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

// CreateUnicornPipeline creates a pipeline to run image clustering on a dataset.
func CreateUnicornPipeline(name string, description string, targetColumns []string, outputLabels []string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDenormalizeStep()).
		Add(NewDatasetToDataframeStep()).
		Add(NewUnicornStep(targetColumns, outputLabels)).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

// CreatePCAFeaturesPipeline creates a pipeline to run feature ranking on an input dataset.
func CreatePCAFeaturesPipeline(name string, description string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDatasetToDataframeStep()).
		Add(NewPCAFeaturesStep()).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

// CreateDenormalizePipeline creates a pipeline to run the denormalize primitive on an input dataset.
func CreateDenormalizePipeline(name string, description string) (*pipeline.PipelineDescription, error) {
	// insantiate the pipeline
	pipeline, err := NewBuilder(name, description).
		Add(NewDenormalizeStep()).
		Add(NewDatasetToDataframeStep()).
		Compile()

	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

func getIndex(allFeatures []*metadata.Variable, name string) (int, error) {
	for _, f := range allFeatures {
		if strings.EqualFold(name, f.Name) {
			return f.Index, nil
		}
	}
	return -1, errors.Errorf("can't find var '%s'", name)
}

// NewTimeSeriesLoaderStep creates a primitive step that reads time series values using a dataframe
// containing a file URI column.  The result is a new dataframe that stores the timetamps as the column headers,
// and the accompanying values for each file as a row.
func NewTimeSeriesLoaderStep(fileColIndex int, timeColIndex int, valueColIndex int) *StepData {
	return NewStepDataWithHyperparameters(
		&pipeline.Primitive{
			Id:         "1689aafa-16dc-4c55-8ad4-76cadcf46086",
			Version:    "0.1.0",
			Name:       "Time series loader",
			PythonPath: "d3m.primitives.distil.TimeSeriesLoader",
			Digest:     "",
		},
		[]string{"produce"},
		map[string]interface{}{
			"file_col_index":  fileColIndex,
			"time_col_index":  timeColIndex,
			"value_col_index": valueColIndex,
		},
	)
}
