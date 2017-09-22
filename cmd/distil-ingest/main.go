package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/unchartedsoftware/deluge"
	delugeElastic "github.com/unchartedsoftware/deluge/elastic/v5"
	"github.com/urfave/cli"
	"gopkg.in/olivere/elastic.v5"

	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/document/d3mdata"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/postgres"
	"github.com/unchartedsoftware/distil-ingest/postgres/model"
	"github.com/unchartedsoftware/distil-ingest/split"
	"github.com/unchartedsoftware/plog"
)

const (
	timeout                  = time.Second * 60 * 5
	errSampleSize            = 10
	metadataIndexName        = "datasets"
	typeSourceClassification = "classification"
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
			Name:  "type-source",
			Value: "schema",
			Usage: "The source for the type information, either `schema` or `classification`",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "classification",
			Value: "",
			Usage: "The classification source path",
		},
		cli.StringFlag{
			Name:  "summary",
			Value: "",
			Usage: "The summary output path",
		},
		cli.StringFlag{
			Name:  "importance",
			Value: "",
			Usage: "The importance source path",
		},
		cli.StringFlag{
			Name:  "es-endpoint",
			Value: "",
			Usage: "The Elasticsearch endpoint",
		},
		cli.StringFlag{
			Name:  "es-metadata-index",
			Value: metadataIndexName,
			Usage: "The Elasticsearch index to ingest metadata into",
		},
		cli.StringFlag{
			Name:  "es-data-index",
			Value: "",
			Usage: "The Elasticsearch index to ingest data into",
		},
		cli.StringFlag{
			Name:  "database",
			Value: "",
			Usage: "The postgres database to use",
		},
		cli.StringFlag{
			Name:  "db-table",
			Value: "",
			Usage: "The database table to ingest into.",
		},
		cli.StringFlag{
			Name:  "db-user",
			Value: "",
			Usage: "The database user to use.",
		},
		cli.StringFlag{
			Name:  "db-password",
			Value: "",
			Usage: "The database password to use for authentication.",
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

		if c.String("es-endpoint") == "" && c.String("database") == "" {
			return cli.NewExitError("missing commandline flag `--es-endpoint` or `--database`", 1)
		}
		if c.String("es-metadata-index") == "" && c.String("db-table") == "" {
			return cli.NewExitError("missing commandline flag `--es-metadata-index` or `--db-table`", 1)
		}
		if c.String("es-data-index") == "" && c.String("db-table") == "" {
			return cli.NewExitError("missing commandline flag `--es-data-index` or `--db-table`", 1)
		}
		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("classification") == "" {
			return cli.NewExitError("missing commandline flag `--classification`", 1)
		}
		if c.String("summary") == "" {
			return cli.NewExitError("missing commandline flag `--summary`", 1)
		}
		if c.String("importance") == "" {
			return cli.NewExitError("missing commandline flag `--importance`", 1)
		}

		config := &conf.Conf{
			ESEndpoint:           c.String("es-endpoint"),
			ESIndex:              c.String("es-data-index"),
			TypeSource:           c.String("type-source"),
			ClassificationPath:   filepath.Clean(c.String("classification")),
			SummaryPath:          filepath.Clean(c.String("summary")),
			ImportancePath:       filepath.Clean(c.String("importance")),
			SchemaPath:           filepath.Clean(c.String("schema")),
			DatasetPath:          filepath.Clean(c.String("dataset")),
			ErrThreshold:         c.Float64("error-threshold"),
			NumActiveConnections: c.Int("num-active-connections"),
			NumWorkers:           c.Int("num-workers"),
			BulkByteSize:         c.Int64("batch-size"),
			ScanBufferSize:       c.Int("scan-size"),
			ClearExisting:        c.Bool("clear-existing"),
			Database:             c.String("database"),
			DBTable:              c.String("db-table"),
			DBUser:               c.String("db-user"),
			DBPassword:           c.String("db-password"),
		}

		// load the metadata
		var meta *metadata.Metadata
		var err error
		if config.TypeSource == typeSourceClassification {
			meta, err = metadata.LoadMetadataFromClassification(
				config.SchemaPath,
				config.ClassificationPath)
		} else {
			meta, err = metadata.LoadMetadataFromSchema(
				config.SchemaPath)
		}

		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// load importance rankings
		colIndices := split.GetNumericColumnIndices(meta)
		err = meta.LoadImportance(config.ImportancePath, "importance_on1stpc", colIndices)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// load summary
		err = meta.LoadSummary(config.SummaryPath, true)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// load stats
		err = meta.LoadDatasetStats(config.DatasetPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		if config.ESEndpoint != "" {
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

			// ingest the metadata
			err = ingestMetadata(metadataIndexName, meta, elasticClient)
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}

			// ingest the data
			err = ingestES(config, delugeClient, meta)
			if err != nil {
				log.Error(err)
				os.Exit(1)
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

		if config.Database != "" {
			err := ingestPostgres(config, meta)
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func ingestMetadata(metadataIndexName string, meta *metadata.Metadata, elasticClient *elastic.Client) error {
	// Create the metadata index if it doesn't exist
	err := metadata.CreateMetadataIndex(elasticClient, metadataIndexName, false)
	if err != nil {
		return err
	}

	// Ingest the dataset info into the metadata index
	err = metadata.IngestMetadata(elasticClient, metadataIndexName, meta)
	if err != nil {
		return err
	}

	return nil
}

func ingestES(config *conf.Conf, delugeClient *delugeElastic.Client, meta *metadata.Metadata) error {
	input, err := deluge.NewFileInput([]string{config.DatasetPath}, nil)
	if err != nil {
		return err
	}

	doc, err := d3mdata.NewD3MData(meta)
	if err != nil {
		return err
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
		return err
	}

	// ingest
	err = ingestor.Ingest()
	if err != nil {
		return err
	}

	return nil
}

func ingestPostgres(config *conf.Conf, meta *metadata.Metadata) error {
	log.Info("Starting ingestion")
	// Connect to the database.
	pg, err := postgres.NewDatabase(config)
	if err != nil {
		return err
	}

	// Drop the current table if requested.
	if config.ClearExisting {
		err = pg.DropTable(config.DBTable)
		if err != nil {
			log.Warn(err)
		}
	}

	// Create the database table.
	var ds *model.Dataset
	if config.TypeSource == typeSourceClassification {
		ds, err = pg.InitializeDataset(meta)
		if err != nil {
			return err
		}
	} else {
		ds, err = pg.ParseMetadata(config.SchemaPath)
		if err != nil {
			return err
		}
	}

	err = pg.InitializeTable(config.DBTable, ds)
	if err != nil {
		return err
	}
	log.Infof("Done table initialization")

	err = pg.StoreMetadata(config.DBTable)
	if err != nil {
		return err
	}
	log.Infof("Done storing metadata")

	err = pg.CreateResultTable(config.DBTable)
	if err != nil {
		return err
	}
	log.Infof("Done creating result table")

	err = pg.CreatePipelineMetadataTables()
	if err != nil {
		return err
	}
	log.Infof("Done creating pipeline metadata tables")

	// Load the data.
	reader, err := os.Open(config.DatasetPath)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		err = pg.IngestRow(config.DBTable, line)
		if err != nil {
			log.Warn(fmt.Sprintf("%v", err))
		}
	}

	log.Info("Done ingestion")

	return nil
}
