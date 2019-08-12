//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/urfave/cli"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-ingest/conf"
	"github.com/uncharted-distil/distil-ingest/metadata"
	"github.com/uncharted-distil/distil-ingest/postgres"
	log "github.com/unchartedsoftware/plog"
)

const (
	timeout                  = time.Second * 60 * 5
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
			Name:  "dataset-folder",
			Value: "",
			Usage: "The root dataset folder name",
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
			Name:  "summary-machine",
			Value: "",
			Usage: "The machine learned summary output path",
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
			Name:  "es-dataset-prefix",
			Value: "",
			Usage: "The Elasticsearch prefix to use for dataset ids",
		},
		cli.StringFlag{
			Name:  "database",
			Value: "",
			Usage: "The postgres database to use",
		},
		cli.StringFlag{
			Name:  "db-host",
			Value: "localhost",
			Usage: "The postgres database hostname - defaults to localhost",
		},
		cli.IntFlag{
			Name:  "db-port",
			Value: 5432,
			Usage: "The postgres database port - defaults to 5432",
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
		cli.IntFlag{
			Name:  "db-batch-size",
			Value: 1000,
			Usage: "The bulk batch size for database ingest",
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
		cli.BoolFlag{
			Name:  "metadata-only",
			Usage: "Create the basic Postgres tables",
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
		cli.Float64Flag{
			Name:  "probability-threshold",
			Value: 0.8,
			Usage: "The threshold below which a classification result will be ignored and the type will default to unknown",
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
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("dataset-folder") == "" {
			return cli.NewExitError("missing commandline flag `--dataset-folder`", 1)
		}
		if c.String("classification") == "" {
			return cli.NewExitError("missing commandline flag `--classification`", 1)
		}
		if c.String("summary") == "" {
			return cli.NewExitError("missing commandline flag `--summary`", 1)
		}
		if c.String("summary-machine") == "" {
			return cli.NewExitError("missing commandline flag `--summary-machine`", 1)
		}
		if c.String("importance") == "" {
			return cli.NewExitError("missing commandline flag `--importance`", 1)
		}

		config := &conf.Conf{
			ESEndpoint:           c.String("es-endpoint"),
			ESIndex:              c.String("es-data-index"),
			ESDatasetPrefix:      c.String("es-dataset-prefix"),
			TypeSource:           c.String("type-source"),
			DatasetFolder:        c.String("dataset-folder"),
			ClassificationPath:   filepath.Clean(c.String("classification")),
			SummaryPath:          filepath.Clean(c.String("summary")),
			SummaryMachinePath:   filepath.Clean(c.String("summary-machine")),
			ImportancePath:       filepath.Clean(c.String("importance")),
			SchemaPath:           filepath.Clean(c.String("schema")),
			DatasetPath:          filepath.Clean(c.String("dataset")),
			ErrThreshold:         c.Float64("error-threshold"),
			ProbabilityThreshold: c.Float64("probability-threshold"),
			NumActiveConnections: c.Int("num-active-connections"),
			NumWorkers:           c.Int("num-workers"),
			BulkByteSize:         c.Int64("batch-size"),
			ScanBufferSize:       c.Int("scan-size"),
			ClearExisting:        c.Bool("clear-existing"),
			MetadataOnly:         c.Bool("metadata-only"),
			Database:             c.String("database"),
			DBTable:              c.String("db-table"),
			DBUser:               c.String("db-user"),
			DBPassword:           c.String("db-password"),
			DBBatchSize:          c.Int("db-batch-size"),
			DBHost:               c.String("db-host"),
			DBPort:               c.Int("db-port"),
		}

		metadata.SetTypeProbabilityThreshold(config.ProbabilityThreshold)

		// load the metadata
		var err error
		var meta *model.Metadata
		if config.SchemaPath == "" || config.SchemaPath == "." {
			log.Infof("Loading metadata from classification file (%s) and raw file (%s)", config.ClassificationPath, config.DatasetPath)
			meta, err = metadata.LoadMetadataFromRawFile(config.DatasetPath, config.ClassificationPath)
		} else if config.TypeSource == typeSourceClassification {
			log.Infof("Loading metadata from classification file (%s) and schema file (%s)", config.ClassificationPath, config.SchemaPath)
			meta, err = metadata.LoadMetadataFromClassification(
				config.SchemaPath,
				config.ClassificationPath,
				true)
		} else {
			log.Infof("Loading metadata from schema file")
			meta, err = metadata.LoadMetadataFromMergedSchema(
				config.SchemaPath)
		}
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
		meta.DatasetFolder = config.DatasetFolder

		// load importance rankings
		err = metadata.LoadImportance(meta, config.ImportancePath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// load summary
		err = metadata.LoadSummary(meta, config.SummaryPath, true)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// load summary
		err = metadata.LoadSummaryMachine(meta, config.SummaryMachinePath)
		if err != nil {
			log.Error(err)
			// NOTE: For now ignore the error as the service may not
			// be able to provide a summary.
			//os.Exit(1)
		}

		// load stats
		err = metadata.LoadDatasetStats(meta, config.DatasetPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// check and fix metadata issues
		err = metadata.VerifyAndUpdate(meta, config.DatasetPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		if config.ESEndpoint != "" && !config.MetadataOnly {
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
			err = ingestMetadata(metadataIndexName, config.ESDatasetPrefix, meta, elasticClient)
			if err != nil {
				log.Error(err)
				os.Exit(1)
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

func ingestMetadata(metadataIndexName string, datasetPrefix string, meta *model.Metadata, elasticClient *elastic.Client) error {
	// Create the metadata index if it doesn't exist
	err := metadata.CreateMetadataIndex(elasticClient, metadataIndexName, false)
	if err != nil {
		return err
	}

	// Ingest the dataset info into the metadata index
	err = metadata.IngestMetadata(elasticClient, metadataIndexName, datasetPrefix, metadata.Seed, meta)
	if err != nil {
		return err
	}

	return nil
}

func ingestPostgres(config *conf.Conf, meta *model.Metadata) error {
	log.Info("Starting ingestion")

	dbTableName := meta.StorageName

	// Connect to the database.
	pg, err := postgres.NewDatabase(config)
	if err != nil {
		return err
	}

	err = pg.CreateSolutionMetadataTables()
	if err != nil {
		return err
	}
	log.Infof("Done creating solution metadata tables")

	if config.MetadataOnly {
		log.Info("Only loading metadata")
		return nil
	}

	// Drop the current table if requested.
	if config.ClearExisting {
		err = pg.DropView(dbTableName)
		if err != nil {
			log.Warn(err)
		}
		err = pg.DropTable(fmt.Sprintf("%s_base", dbTableName))
		if err != nil {
			log.Warn(err)
		}
	}

	// Create the database table.
	ds, err := pg.InitializeDataset(meta)
	if err != nil {
		return err
	}

	err = pg.InitializeTable(dbTableName, ds)
	if err != nil {
		return err
	}
	log.Infof("Done table initialization")

	err = pg.StoreMetadata(dbTableName)
	if err != nil {
		return err
	}
	log.Infof("Done storing metadata")

	err = pg.CreateResultTable(dbTableName)
	if err != nil {
		return err
	}
	log.Infof("Done creating result table")

	// Load the data.
	// open the file
	csvFile, err := os.Open(config.DatasetPath)
	if err != nil {
		return err
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)

	// skip header
	reader.Read()
	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Raw schema source will have header row.
		if count > 0 || meta.SchemaSource != model.SchemaSourceRaw {
			err = pg.AddWordStems(line)
			if err != nil {
				log.Warn(fmt.Sprintf("%v", err))
			}

			err = pg.IngestRow(dbTableName, line)
			if err != nil {
				log.Warn(fmt.Sprintf("%v", err))
			}
		}
		count = count + 1
	}

	err = pg.InsertRemainingRows()
	if err != nil {
		log.Warn(fmt.Sprintf("%v", err))
	}

	log.Info("Done ingestion")

	return nil
}
