package main

import (
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/unchartedsoftware/deluge"
	delugeElastic "github.com/unchartedsoftware/deluge/elastic/v5"
	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/document/d3mdata"
	"github.com/unchartedsoftware/distil-ingest/merge"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/plog"
	elastic "gopkg.in/olivere/elastic.v5"
)

const (
	timeout           = time.Second * 60 * 5
	errSampleSize     = 10
	metadataIndexName = "datasets"
	d3mIndexColName   = "d3mIndex"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	// parse flags into config struct
	config, err := conf.ParseCommandLine()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// create elasticsearch client
	delugeClient, err := delugeElastic.NewClient(
		delugeElastic.SetURL(config.ESEndpoint),
		delugeElastic.SetHTTPClient(&http.Client{Timeout: timeout}),
		delugeElastic.SetMaxRetries(10),
		delugeElastic.SetSniff(false),
		delugeElastic.SetGzip(true))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// create elasticsearch client
	elasticClient, err := elastic.NewClient(
		elastic.SetURL(config.ESEndpoint),
		elastic.SetHttpClient(&http.Client{Timeout: timeout}),
		elastic.SetMaxRetries(10),
		elastic.SetSniff(false),
		elastic.SetGzip(true))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Create the metadata index if it doesn't exist
	err = metadata.CreateMetadataIndex(metadataIndexName, false, elasticClient)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Ingest the dataset info into the metadata index
	err = metadata.IngestMetadata(metadataIndexName, config.DatasetPath+"/data/dataSchema.json", elasticClient)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Merge targets into training data before ingest
	indices, err := merge.GetColIndices(config.DatasetPath+"/data/dataSchema.json", d3mIndexColName)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	merge.LeftJoin(config.DatasetPath+"/data/trainData.csv", indices.LeftColIdx,
		config.DatasetPath+"/data/trainTargets.csv", indices.RightColIdx,
		config.DatasetPath+"/data/merged.csv", true)

	// Filesystem Input
	excludes := []string{
		"dataDescription.txt",
		"dataSchema.json",
		"trainData.csv",
		"trainTargets.csv",
		"testData.csv",
	}

	input, err := deluge.NewFileInput(config.DatasetPath+"/data", excludes)

	doc := d3mdata.NewD3MData(config.DatasetPath + "/data/dataSchema.json")

	// create ingestor
	ingestor, err := deluge.NewIngestor(
		deluge.SetDocument(doc),
		deluge.SetInput(input),
		deluge.SetClient(delugeClient),
		deluge.SetIndex(config.ESIndex),
		deluge.SetErrorThreshold(config.ErrThreshold),
		deluge.SetActiveConnections(config.NumActiveConnections),
		deluge.SetNumWorkers(config.NumWorkers),
		deluge.SetBulkByteSize(config.BulkByteSize),
		deluge.SetScanBufferSize(config.ScanBufferSize),
		deluge.ClearExistingIndex(config.ClearExisting),
		deluge.SetNumReplicas(1))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// ingest
	err = ingestor.Ingest()
	if err != nil {
		log.Error(err)
	}

	// check errors
	errs := deluge.DocErrs()
	if len(errs) > 0 {
		log.Errorf("Failed ingesting %d documents, logging sample size of %d errors:",
			len(errs),
			errSampleSize)
		for _, err := range deluge.SampleDocErrs(errSampleSize) {
			log.Error(err)
		}
	}

}
