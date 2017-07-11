package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/deluge"
	delugeElastic "github.com/unchartedsoftware/deluge/elastic/v5"
	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/document/d3mdata"
	"github.com/unchartedsoftware/distil-ingest/merge"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/postgres"
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

	// Based on parameters provided, ingest to the right target.
	if config.Database != "" {
		ingestPostgres(config)
	} else {
		ingestElastic(config)
	}
}

func ingestPostgres(config *conf.Conf) {
	log.Info("Starting ingestion")
	// Connect to the database.
	pg, err := postgres.NewDatabase(config)
	if err != nil {
		log.Error(err)
	}

	// Merge data since the merged data is ingested.
	mergeData(config)

	// Drop the current table if requested.
	if config.ClearExisting {
		err = pg.DropTable(config.DBTable)
		if err != nil {
			log.Warn(err)
		}
	}

	// Create the database table.
	err = pg.InitializeTable(config.DBTable, config.DatasetPath+"/data/dataSchema.json")
	if err != nil {
		log.Error(err)
	}
	log.Infof("Done table initialization")

	// Load the data.
	reader, err := os.Open(config.DatasetPath + "/data/merged.csv")
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		err = pg.IngestRow(config.DBTable, line)
		if err != nil {
			log.Warn(fmt.Sprintf("%v", err))
		}
	}

	log.Info("Done ingestion")
}

func ingestElastic(config *conf.Conf) {

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
		log.Error(errors.Cause(err))
		os.Exit(1)
	}

	// Ingest the dataset info into the metadata index
	err = metadata.IngestMetadata(metadataIndexName, config.DatasetPath+"/data/dataSchema.json", elasticClient)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Merge targets into training data before ingest
	mergeData(config)

	// Filesystem Input
	excludes := []string{
		"dataDescription.txt",
		"dataSchema.json",
		"trainData.csv",
		"trainTargets.csv",
		"testData.csv",
	}

	input, err := deluge.NewFileInput([]string{config.DatasetPath+"/data"}, excludes)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	doc, err := d3mdata.NewD3MData(config.DatasetPath + "/data/dataSchema.json")
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

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

func mergeData(config *conf.Conf) {
	log.Infof("Merging data in %s", config.DatasetPath)
	indices, err := merge.GetColIndices(config.DatasetPath+"/data/dataSchema.json", d3mIndexColName)
	if err != nil {
		log.Error(errors.Cause(err))
		os.Exit(1)
	}
	merge.LeftJoin(config.DatasetPath+"/data/trainData.csv", indices.LeftColIdx,
		config.DatasetPath+"/data/trainTargets.csv", indices.RightColIdx,
		config.DatasetPath+"/data/merged.csv", true)
	log.Infof("Done merging data in %s", config.DatasetPath)
}
