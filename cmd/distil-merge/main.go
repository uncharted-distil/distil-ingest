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
	"path/filepath"
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
	app.Name = "distil-merge"
	app.Version = "0.1.0"
	app.Usage = "Merge D3M training datasets"
	app.UsageText = "distil-merge --endpoint=<url> --dataset=<filepath> --schema=<filepath> --input=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The dataset schema file path",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataet schema path",
		},
		cli.StringFlag{
			Name:  "endpoint",
			Value: "",
			Usage: "The pipeline runner endpoint",
		},
		cli.StringFlag{
			Name:  "input",
			Value: "",
			Usage: "The clustering input path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The merged output folder",
		},
	}
	app.Action = func(c *cli.Context) error {

		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--endpoint`", 1)
		}
		if c.String("output") == "" {
			return cli.NewExitError("missing commandline flag `--output`", 1)
		}

		output := filepath.Clean(c.String("output"))
		endpoint := filepath.Clean(c.String("endpoint"))
		dataset := filepath.Clean(c.String("dataset"))
		schema := filepath.Clean(c.String("schema"))
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

		// merge the dataset into a single file
		mergedPath, err := task.Merge(schema, dataset, ingestConfig)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Merged data written to %s", mergedPath)

		return nil
	}
	// run app
	app.Run(os.Args)
}
