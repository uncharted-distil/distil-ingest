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
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-compute/model"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil/api/env"
	"github.com/uncharted-distil/distil/api/task"
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

		// initialize config
		typeSource := c.String("type-source")
		metadataOnly := c.Bool("metadata-only")
		config, err := env.LoadConfig()
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		config.ElasticEndpoint = c.String("es-endpoint")
		config.ESDatasetsIndex = c.String("es-data-index")
		config.ElasticDatasetPrefix = c.String("es-dataset-prefix")
		config.ClassificationOutputPath = filepath.Clean(c.String("classification"))
		config.SummaryPath = filepath.Clean(c.String("summary"))
		config.SummaryMachinePath = filepath.Clean(c.String("summary-machine"))
		config.RankingOutputPath = filepath.Clean(c.String("importance"))
		config.SchemaPath = filepath.Clean(c.String("schema"))
		config.ClassificationProbabilityThreshold = c.Float64("probability-threshold")
		config.PostgresDatabase = c.String("database")
		config.PostgresUser = c.String("db-user")
		config.PostgresPassword = c.String("db-password")
		config.PostgresHost = c.String("db-host")
		config.PostgresPort = c.Int("db-port")

		ingestConfig := task.NewConfig(config)

		metadata.SetTypeProbabilityThreshold(config.ClassificationProbabilityThreshold)

		// load the metadata
		var meta *model.Metadata
		if config.SchemaPath == "" || config.SchemaPath == "." {
			log.Infof("Loading metadata from classification file (%s) and raw file (%s)", config.ClassificationOutputPath, config.SchemaPath)
			meta, err = metadata.LoadMetadataFromRawFile(config.SchemaPath, config.ClassificationOutputPath)
		} else if typeSource == typeSourceClassification {
			log.Infof("Loading metadata from classification file (%s) and schema file (%s)", config.ClassificationOutputPath, config.SchemaPath)
			meta, err = metadata.LoadMetadataFromClassification(
				config.SchemaPath,
				config.ClassificationOutputPath,
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
		meta.DatasetFolder = config.SchemaPath

		// load importance rankings
		err = metadata.LoadImportance(meta, config.RankingOutputPath)
		if err != nil {
			log.Error(err)
		}

		// load summary
		metadata.LoadSummary(meta, config.SummaryPath, true)

		// load summary
		err = metadata.LoadSummaryMachine(meta, config.SummaryMachinePath)
		if err != nil {
			log.Error(err)
			// NOTE: For now ignore the error as the service may not
			// be able to provide a summary.
			//os.Exit(1)
		}

		// load stats
		err = metadata.LoadDatasetStats(meta, config.SchemaPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// check and fix metadata issues
		_, err = metadata.VerifyAndUpdate(meta, config.SchemaPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		if config.ElasticEndpoint != "" && !metadataOnly {
			// create elasticsearch client
			elasticClient, err := elastic.NewClient(
				elastic.SetURL(config.ElasticEndpoint),
				elastic.SetHttpClient(&http.Client{Timeout: timeout}),
				elastic.SetMaxRetries(10),
				elastic.SetSniff(false),
				elastic.SetGzip(true))
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}

			// ingest the metadata
			err = ingestMetadata(metadataIndexName, config.ElasticDatasetPrefix, meta, elasticClient)
			if err != nil {
				log.Error(err)
				os.Exit(1)
			}
		}

		if config.PostgresDatabase != "" {
			err := ingestPostgres(&config, ingestConfig, meta)
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

func ingestPostgres(config *env.Config, ingestConfig *task.IngestTaskConfig, meta *model.Metadata) error {
	log.Info("Starting ingest")

	err := task.IngestDataset(metadata.Seed, nil, nil, "", config.SchemaPath, nil, ingestConfig)
	if err != nil {
		return err
	}

	return nil
}
