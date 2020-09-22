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
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-compute/model"
	distilds "github.com/uncharted-distil/distil/api/dataset"
	es "github.com/uncharted-distil/distil/api/elastic"
	"github.com/uncharted-distil/distil/api/env"
	api "github.com/uncharted-distil/distil/api/model"
	elastic "github.com/uncharted-distil/distil/api/model/storage/elastic"
	pg "github.com/uncharted-distil/distil/api/model/storage/postgres"
	"github.com/uncharted-distil/distil/api/postgres"
	"github.com/uncharted-distil/distil/api/task"
	log "github.com/unchartedsoftware/plog"
)

const (
	timeout                  = time.Second * 60 * 5
	metadataIndexName        = "datasets"
	modelIndexName           = "models"
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
			Name:  "es-model-index",
			Value: modelIndexName,
			Usage: "The Elasticsearch index to ingest models into",
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
		cli.BoolFlag{
			Name:  "metadata-only",
			Usage: "Create the basic Postgres tables",
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
		if c.String("es-model-index") == "" && c.String("db-table") == "" {
			return cli.NewExitError("missing commandline flag `--es-model-index` or `--db-table`", 1)
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
		dataset := c.String("dataset")
		metadataOnly := c.Bool("metadata-only")
		config, err := env.LoadConfig()
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		config.ElasticEndpoint = c.String("es-endpoint")
		config.ESDatasetsIndex = c.String("es-metadata-index")
		config.ESModelsIndex = c.String("es-model-index")
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
		if config.ElasticEndpoint != "" && !metadataOnly {
			// ingest the metadata with retries in case of transient errors
			for i := 0; i < 3; i++ {
				err = ingestMetadata(dataset, &config, ingestConfig)
				if err != nil {
					log.Warnf("error on attempt %d: %+v", i, err)
				} else {
					break
				}

				time.Sleep(10 * time.Second)
			}
			if err != nil {
				log.Errorf("maximum number of retries reached with error")
				os.Exit(1)
			}
		} else if config.PostgresDatabase != "" {
			err = ingestPostgres(dataset, &config, ingestConfig)
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

func ingestMetadata(dataset string, config *env.Config, ingestConfig *task.IngestTaskConfig) error {
	log.Infof("ingesting metadata for dataset %s", dataset)
	esClientCtor := es.NewClient(ingestConfig.ESEndpoint, true)
	log.Infof("creating datasets index '%s'", config.ESDatasetsIndex)
	storageCtor := elastic.NewMetadataStorage(config.ESDatasetsIndex, true, esClientCtor)
	storage, err := storageCtor()
	if err != nil {
		return err
	}

	log.Infof("creating models index '%s'", config.ESModelsIndex)
	storageModelCtor := elastic.NewExportedModelStorage(config.ESModelsIndex, true, esClientCtor)
	_, err = storageModelCtor()
	if err != nil {
		return err
	}

	_, err = task.IngestMetadata(config.SchemaPath, config.SchemaPath, nil, storage,
		metadata.Seed, nil, api.DatasetTypeModelling, ingestConfig, true, true)
	if err != nil {
		return err
	}

	meta, err := metadata.LoadMetadataFromOriginalSchema(config.SchemaPath, false)
	if err != nil {
		return err
	}

	if isRemoteSensing(meta) {
		log.Infof("remote sensing dataset detected, so setting grouping info")
		// set the remote sensing group
		rawGrouping := distilds.CreateSatelliteGrouping()
		err = task.SetGroups(meta.ID, rawGrouping, storage, ingestConfig)
		if err != nil {
			return err
		}
	} else {
		postgresClientCtor := postgres.NewClient(config.PostgresHost, config.PostgresPort, config.PostgresUser, config.PostgresPassword,
			config.PostgresDatabase, config.PostgresLogLevel, false)
		postgresBatchClientCtor := postgres.NewClient(config.PostgresHost, config.PostgresPort, config.PostgresUser, config.PostgresPassword,
			config.PostgresDatabase, "error", true)

		dataStorageCtor := pg.NewDataStorage(postgresClientCtor, postgresBatchClientCtor, storageCtor)
		dataStorage, err := dataStorageCtor()
		if err != nil {
			return err
		}

		err = task.VerifySuggestedTypes(dataset, dataStorage, storage)
		if err != nil {
			return err
		}
	}
	log.Infof("done ingesting metadata for dataset %s", dataset)

	return nil
}

func ingestPostgres(dataset string, config *env.Config, ingestConfig *task.IngestTaskConfig) error {
	log.Infof("starting postgres ingest for dataset %s", dataset)

	err := task.IngestPostgres(config.SchemaPath, config.SchemaPath, metadata.Seed, ingestConfig, true, true, true)
	if err != nil {
		return err
	}
	log.Infof("done postgres ingest for dataset %s", dataset)

	return nil
}

func isRemoteSensing(meta *model.Metadata) bool {
	// check for band and image file variables
	vars := map[string]bool{}
	for _, v := range meta.GetMainDataResource().Variables {
		vars[v.Name] = true
	}

	return vars["band"] && vars["image_file"]
}
