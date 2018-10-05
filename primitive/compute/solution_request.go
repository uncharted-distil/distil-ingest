package compute

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/unchartedsoftware/distil-ingest/pipeline"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/description"
	"github.com/unchartedsoftware/plog"
)

const (
	defaultResourceID       = "0"
	defaultExposedOutputKey = "outputs.0"
	trainTestSplitThreshold = 0.9
	// SolutionPendingStatus represents that the solution request has been acknoledged by not yet sent to the API
	SolutionPendingStatus = "SOLUTION_PENDING"
	// SolutionRunningStatus represents that the solution request has been sent to the API.
	SolutionRunningStatus = "SOLUTION_RUNNING"
	// SolutionErroredStatus represents that the solution request has terminated with an error.
	SolutionErroredStatus = "SOLUTION_ERRORED"
	// SolutionCompletedStatus represents that the solution request has completed successfully.
	SolutionCompletedStatus = "SOLUTION_COMPLETED"
	// RequestPendingStatus represents that the solution request has been acknoledged by not yet sent to the API
	RequestPendingStatus = "REQUEST_PENDING"
	// RequestRunningStatus represents that the solution request has been sent to the API.
	RequestRunningStatus = "REQUEST_RUNNING"
	// RequestErroredStatus represents that the solution request has terminated with an error.
	RequestErroredStatus = "REQUEST_ERRORED"
	// RequestCompletedStatus represents that the solution request has completed successfully.
	RequestCompletedStatus = "REQUEST_COMPLETED"
)

var (
	// folder for dataset data exchanged with TA2
	datasetDir string
	// folder containing the input dataset
	inputDir string
)

// SetDatasetDir sets the output data dir
func SetDatasetDir(dir string) {
	datasetDir = dir
}

// SetInputDir sets the input data dir
func SetInputDir(dir string) {
	inputDir = dir
}

func newStatusChannel() chan SolutionStatus {
	// NOTE: WE BUFFER THE CHANNEL TO A SIZE OF 1 HERE SO THAT THE INITIAL
	// PERSIST DOES NOT DEADLOCK
	return make(chan SolutionStatus, 1)
}

// SolutionRequest represents a solution search request.
type SolutionRequest struct {
	Dataset          string   `json:"dataset"`
	Index            string   `json:"index"`
	TargetFeature    string   `json:"target"`
	Task             string   `json:"task"`
	SubTask          string   `json:"subTask"`
	MaxSolutions     int32    `json:"maxSolutions"`
	MaxTime          int64    `json:"maxTime"`
	Metrics          []string `json:"metrics"`
	mu               *sync.Mutex
	wg               *sync.WaitGroup
	requestChannel   chan SolutionStatus
	solutionChannels []chan SolutionStatus
	listener         SolutionStatusListener
	finished         chan error
}

// NewSolutionRequest instantiates a new SolutionRequest.
func NewSolutionRequest(data []byte) (*SolutionRequest, error) {
	req := &SolutionRequest{
		mu:             &sync.Mutex{},
		wg:             &sync.WaitGroup{},
		finished:       make(chan error),
		requestChannel: newStatusChannel(),
	}
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// SolutionStatus represents a solution status.
type SolutionStatus struct {
	Progress   string    `json:"progress"`
	RequestID  string    `json:"requestId"`
	SolutionID string    `json:"solutionId"`
	ResultID   string    `json:"resultId"`
	Error      error     `json:"error"`
	Timestamp  time.Time `json:"timestamp"`
}

// SolutionStatusListener executes on a new solution status.
type SolutionStatusListener func(status SolutionStatus)

func (s *SolutionRequest) addSolution(c chan SolutionStatus) {
	s.wg.Add(1)
	s.mu.Lock()
	s.solutionChannels = append(s.solutionChannels, c)
	if s.listener != nil {
		go s.listenOnStatusChannel(c)
	}
	s.mu.Unlock()
}

func (s *SolutionRequest) completeSolution() {
	s.wg.Done()
}

func (s *SolutionRequest) waitOnSolutions() {
	s.wg.Wait()
}

func (s *SolutionRequest) listenOnStatusChannel(statusChannel chan SolutionStatus) {
	for {
		// read status from, channel
		status := <-statusChannel
		// execute callback
		s.listener(status)
	}
}

// Listen listens ont he solution requests for new solution statuses.
func (s *SolutionRequest) Listen(listener SolutionStatusListener) error {
	s.listener = listener
	s.mu.Lock()
	// listen on main request channel
	go s.listenOnStatusChannel(s.requestChannel)
	// listen on individual solution channels
	for _, c := range s.solutionChannels {
		go s.listenOnStatusChannel(c)
	}
	s.mu.Unlock()
	return <-s.finished
}

func (s *SolutionRequest) createSearchSolutionsRequest(preprocessing *pipeline.PipelineDescription,
	datasetURI string, userAgent string) (*pipeline.SearchSolutionsRequest, error) {
	return createSearchSolutionsRequest(preprocessing, datasetURI, userAgent, s.TargetFeature, s.Dataset, s.Metrics, s.Task, s.SubTask, s.MaxTime)
}

func createSearchSolutionsRequest(preprocessing *pipeline.PipelineDescription,
	datasetURI string, userAgent string, targetFeature string, dataset string, metrics []string, task string, subTask string, maxTime int64) (*pipeline.SearchSolutionsRequest, error) {

	return &pipeline.SearchSolutionsRequest{
		Problem: &pipeline.ProblemDescription{
			Problem: &pipeline.Problem{
				TaskType:           convertTaskTypeFromTA3ToTA2(task),
				TaskSubtype:        convertTaskSubTypeFromTA3ToTA2(subTask),
				PerformanceMetrics: convertMetricsFromTA3ToTA2(metrics),
			},
			Inputs: []*pipeline.ProblemInput{
				{
					DatasetId: convertDatasetTA3ToTA2(dataset),
					Targets:   convertTargetFeaturesTA3ToTA2(targetFeature, -1),
				},
			},
		},

		// Our agent/version info
		UserAgent: userAgent,
		Version:   GetAPIVersion(),

		// Requested max time for solution search - not guaranteed to be honoured
		TimeBound: float64(maxTime),

		// we accept dataset and csv uris as return types
		AllowedValueTypes: []pipeline.ValueType{
			pipeline.ValueType_DATASET_URI,
			pipeline.ValueType_CSV_URI,
		},

		// URI of the input dataset
		Inputs: []*pipeline.Value{
			{
				Value: &pipeline.Value_DatasetUri{
					DatasetUri: datasetURI,
				},
			},
		},

		Template: preprocessing,
	}, nil
}

// createPreprocessingPipeline creates pipeline to enfore user feature selection and typing
func (s *SolutionRequest) createPreprocessingPipeline(targetVariable string) (*pipeline.PipelineDescription, error) {
	uuid := uuid.NewV4()
	name := fmt.Sprintf("preprocessing-%s-%s", s.Dataset, uuid.String())
	desc := fmt.Sprintf("Preprocessing pipeline capturing user feature selection and type information. Dataset: `%s` ID: `%s`", s.Dataset, uuid.String())

	preprocessingPipeline, err := description.CreateUserDatasetPipeline(name, desc, targetVariable)
	if err != nil {
		return nil, err
	}

	return preprocessingPipeline, nil
}

func (s *SolutionRequest) createProduceSolutionRequest(datasetURI string, fittedSolutionID string) *pipeline.ProduceSolutionRequest {
	return &pipeline.ProduceSolutionRequest{
		FittedSolutionId: fittedSolutionID,
		Inputs: []*pipeline.Value{
			{
				Value: &pipeline.Value_DatasetUri{
					DatasetUri: datasetURI,
				},
			},
		},
		ExposeOutputs: []string{defaultExposedOutputKey},
		ExposeValueTypes: []pipeline.ValueType{
			pipeline.ValueType_CSV_URI,
		},
	}
}

func (s *SolutionRequest) dispatchSolution(statusChan chan SolutionStatus, client *Client, searchID string, solutionID string, dataset string, datasetURITrain string, datasetURITest string) error {

	// score solution
	solutionScoreResponses, err := client.GenerateSolutionScores(context.Background(), solutionID, datasetURITest, s.Metrics)
	if err != nil {
		return err
	}

	// persist the scores
	for _, response := range solutionScoreResponses {
		// only persist scores from COMPLETED responses
		if response.Progress.State == pipeline.ProgressState_COMPLETED {
			log.Infof("scoring complete")
		}
	}

	// fit solution
	var fitResults []*pipeline.GetFitSolutionResultsResponse
	fitResults, err = client.GenerateSolutionFit(context.Background(), solutionID, datasetURITrain)
	if err != nil {
		return err
	}

	// find the completed result and get the fitted solution ID out
	var fittedSolutionID string
	for _, result := range fitResults {
		if result.GetFittedSolutionId() != "" {
			fittedSolutionID = result.GetFittedSolutionId()
			break
		}
	}
	if fittedSolutionID == "" {
		return errors.Errorf("no fitted solution ID for solution `%s`", solutionID)
	}

	// generate predictions
	produceSolutionRequest := s.createProduceSolutionRequest(datasetURITest, fittedSolutionID)

	// generate predictions
	predictionResponses, err := client.GeneratePredictions(context.Background(), produceSolutionRequest)
	if err != nil {
		return err
	}

	for _, response := range predictionResponses {

		if response.Progress.State != pipeline.ProgressState_COMPLETED {
			// only persist completed responses
			continue
		}
	}

	return nil
}

func (s *SolutionRequest) dispatchRequest(client *Client, searchID string, dataset string, datasetURITrain string, datasetURITest string) error {

	// search for solutions, this wont return until the search finishes or it times out
	err := client.SearchSolutions(context.Background(), searchID, func(solution *pipeline.GetSearchSolutionsResultsResponse) {
		// create a new status channel for the solution
		c := newStatusChannel()
		// add the solution to the request
		s.addSolution(c)
		// dispatch it
		s.dispatchSolution(c, client, searchID, solution.SolutionId, dataset, datasetURITrain, datasetURITest)
		// once done, mark as complete
		s.completeSolution()
	})

	// update request status
	if err != nil {
		return err
	}

	// wait until all are complete and the search has finished / timed out
	s.waitOnSolutions()

	// end search
	s.finished <- client.EndSearch(context.Background(), searchID)

	return nil
}

// PersistAndDispatch persists the solution request and dispatches it.
func (s *SolutionRequest) PersistAndDispatch(client *Client) error {

	// perist the datasets and get URI
	datasetPathTrain, datasetPathTest, err := PersistOriginalData(s.Dataset, D3MDataSchema, inputDir, datasetDir)
	if err != nil {
		return err
	}

	// make sure the path is absolute and contains the URI prefix
	datasetPathTrain, err = filepath.Abs(datasetPathTrain)
	if err != nil {
		return err
	}
	datasetPathTrain = fmt.Sprintf("file://%s", datasetPathTrain)
	datasetPathTest, err = filepath.Abs(datasetPathTest)
	if err != nil {
		return err
	}
	datasetPathTest = fmt.Sprintf("file://%s", datasetPathTest)

	// generate the pre-processing pipeline to enforce feature selection and semantic type changes
	var preprocessing *pipeline.PipelineDescription
	if !client.SkipPreprocessing {
		preprocessing, err = s.createPreprocessingPipeline(s.TargetFeature)
		if err != nil {
			return err
		}
	}

	// create search solutions request
	searchRequest, err := s.createSearchSolutionsRequest(preprocessing, datasetPathTrain, client.UserAgent)
	if err != nil {
		return err
	}

	// start a solution searchID
	requestID, err := client.StartSearch(context.Background(), searchRequest)
	if err != nil {
		return err
	}

	// dispatch search request
	go s.dispatchRequest(client, requestID, s.Dataset, datasetPathTrain, datasetPathTest)

	return nil
}

// CreateSearchSolutionRequest creates a search solution request, including
// the pipeline steps required to process the data.
func CreateSearchSolutionRequest(
	target string, sourceURI string, dataset string,
	userAgent string, skipPreprocessing bool) (*pipeline.SearchSolutionsRequest, error) {
	uuid := uuid.NewV4()
	name := fmt.Sprintf("preprocessing-%s-%s", dataset, uuid.String())
	desc := fmt.Sprintf("Preprocessing pipeline capturing user feature selection and type information. Dataset: `%s` ID: `%s`", dataset, uuid.String())

	var err error
	var preprocessingPipeline *pipeline.PipelineDescription
	if !skipPreprocessing {
		preprocessingPipeline, err = description.CreateUserDatasetPipeline(name, desc, target)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create preprocessing pipeline")
		}
	}

	// create search solutions request
	searchRequest, err := createSearchSolutionsRequest(preprocessingPipeline, sourceURI, userAgent, target, dataset, nil, "", "", 600)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create search solution request")
	}

	return searchRequest, nil
}
