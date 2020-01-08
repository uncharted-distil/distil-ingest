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
	"path"
	"runtime"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil/api/env"
	"github.com/uncharted-distil/distil/api/task"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-featurize"
	app.Version = "0.1.0"
	app.Usage = "Featurize D3M datasets"
	app.UsageText = "distil-featurize --endpoint=<url> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "endpoint",
			Value: "",
			Usage: "The pipeline runner endpoint",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The schema source path",
		},
		cli.StringFlag{
			Name:  "filetype",
			Value: "csv",
			Usage: "The dataset file type",
		},
		cli.StringFlag{
			Name:  "media-path",
			Value: "",
			Usage: "The path to the folder containing the media subfolder that is accessible for featurization",
		},
		cli.StringFlag{
			Name:  "input",
			Value: "",
			Usage: "The clustering input path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The path to use as output for the featurized data",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
		cli.Float64Flag{
			Name:  "threshold",
			Value: 0.2,
			Usage: "Confidence threshold to use for labels",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--endpoint`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		endpoint := c.String("endpoint")
		datasetPath := c.String("dataset")
		//mediaPath := c.String("media-path")
		//outputData := c.String("output-data")
		schemaPath := c.String("schema")
		output := c.String("output")
		input := c.String("input")
		//hasHeader := c.Bool("has-header")
		//rootDataPath := path.Dir(datasetPath)
		//threshold := c.Float64("threshold")

		// initialize config
		log.Infof("Using TA2 interface at `%s` ", endpoint)
		config, err := env.LoadConfig()
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		config.SolutionComputeEndpoint = endpoint
		config.D3MInputDir = input
		config.D3MOutputDir = path.Dir(path.Dir(path.Dir(path.Dir(output))))

		err = env.Initialize(&config)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		ingestConfig := task.NewConfig(config)

		// initialize client
		client, err := task.NewDefaultClient(config, "distil-ingest", nil)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		defer client.Close()
		task.SetClient(client)

		// create featurizer
		featurePath, err := task.Featurize(metadata.Seed, schemaPath, "", datasetPath, ingestConfig)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Featurized data written to %s", featurePath)

		return nil
	}
	// run app
	app.Run(os.Args)
}
