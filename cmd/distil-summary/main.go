//
//   Copyright © 2021 Uncharted Software Inc.
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

	"github.com/uncharted-distil/distil/api/compute"
	"github.com/uncharted-distil/distil/api/env"
	"github.com/uncharted-distil/distil/api/task"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-summary"
	app.Version = "0.1.0"
	app.Usage = "Summarize D3M datasets"
	app.UsageText = "distil-summary --endpoint=<url> --dataset=<filepath> --schema=<filepath> --input=<filepath> --output=<filepath>"
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
			Name:  "input",
			Value: "",
			Usage: "The clustering input path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The summary output file path",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("output") == "" {
			return cli.NewExitError("missing commandline flag `--output`", 1)
		}

		endpoint := c.String("endpoint")
		dataset := c.String("dataset")
		schemaPath := c.String("schema")
		output := c.String("output")
		input := c.String("input")

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

		// initialize the pipeline cache and queue
		compute.InitializeCache(config.PipelineCacheFilename, true)
		compute.InitializeQueue(&config)

		// initialize client
		client, err := task.NewDefaultClient(config, "distil-ingest", nil)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		defer client.Close()
		task.SetClient(client)

		// summarize the dataset
		summaryOutput, err := task.Summarize(schemaPath, dataset, ingestConfig)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("summarized data written to %s", summaryOutput)

		return nil
	}
	// run app
	app.Run(os.Args)
}
