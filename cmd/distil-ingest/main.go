package main

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/deluge"
	delugeElastic "github.com/unchartedsoftware/deluge/elastic/v5"
	"github.com/urfave/cli"
	"gopkg.in/olivere/elastic.v5"

	"github.com/unchartedsoftware/distil-ingest/document/d3mdata"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/plog"
)

const (
	timeout           = time.Second * 60 * 5
	errSampleSize     = 10
	metadataIndexName = "datasets"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-ingest"
	app.Version = "0.1.0"
	app.Usage = "Ingest D3M training datasets into elasticsearch"
	app.UsageText = "distil-ingest --schema=<filepath> --dataset=<filepath> --es-endpoint=<url> --es-index=<index>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The dataset schema file path",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "es-endpoint",
			Value: "",
			Usage: "The Elasticsearch endpoint",
		},
		cli.StringFlag{
			Name:  "es-index",
			Value: "",
			Usage: "The Elasticsearch index to ingest into",
		},
		cli.Int64Flag{
			Name:  "batch-size",
			Value: 1024 * 1024 * 20,
			Usage: "The bulk batch size in bytes",
		},
		cli.IntFlag{
			Name:  "scan-size",
			Value: 1024 * 1024 * 2,
			Usage: "The size of the buffer allocated for each input row",
		},
		cli.BoolFlag{
			Name:  "clear-existing",
			Usage: "Clear index before ingest",
		},
		cli.IntFlag{
			Name:  "num-workers",
			Value: 8,
			Usage: "The worker pool size",
		},
		cli.IntFlag{
			Name:  "num-active-connections",
			Value: 8,
			Usage: "The number of concurrent outgoing connections",
		},
		cli.Float64Flag{
			Name:  "error-threshold",
			Value: 0.01,
			Usage: "The percentage threshold of unsuccessful documents which when passed will end ingestion",
		},
	}
	app.Action = func(c *cli.Context) error {

		if c.String("es-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--es-endpoint`", 1)
		}
		if c.String("es-index") == "" {
			return cli.NewExitError("missing commandline flag `--es-index`", 1)
		}
		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		esEndpoint := c.String("es-endpoint")
		esIndex := c.String("es-index")
		schemaPath := filepath.Clean(c.String("schema"))
		datasetPath := filepath.Clean(c.String("dataset"))
		errThreshold := c.Float64("error-threshold")
		numActiveConnections := c.Int("num-active-connections")
		numWorkers := c.Int("num-workers")
		bulkByteSize := c.Int64("batch-size")
		scanBufferSize := c.Int("scan-size")
		clearExisting := c.Bool("clear-existing")

		// create elasticsearch client
		delugeClient, err := delugeElastic.NewClient(
			delugeElastic.SetURL(esEndpoint),
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
			elastic.SetURL(esEndpoint),
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
		err = metadata.IngestMetadata(metadataIndexName, schemaPath, elasticClient)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		input, err := deluge.NewFileInput([]string{datasetPath}, nil)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		doc, err := d3mdata.NewD3MData(schemaPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// create ingestor
		ingestor, err := deluge.NewIngestor(
			deluge.SetDocument(doc),
			deluge.SetInput(input),
			deluge.SetClient(delugeClient),
			deluge.SetIndex(esIndex),
			deluge.SetErrorThreshold(errThreshold),
			deluge.SetActiveConnections(numActiveConnections),
			deluge.SetNumWorkers(numWorkers),
			deluge.SetBulkByteSize(bulkByteSize),
			deluge.SetScanBufferSize(scanBufferSize),
			deluge.ClearExistingIndex(clearExisting),
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

		return nil
	}
	// run app
	app.Run(os.Args)
}
