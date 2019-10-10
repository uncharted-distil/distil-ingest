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
	"time"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-ingest/primitive"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-cluster"
	app.Version = "0.1.0"
	app.Usage = "Cluster D3M datasets"
	app.UsageText = "distil-cluster --endpoint=<url> --dataset=<filepath> --output=<filepath>"
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
			Name:  "output",
			Value: "",
			Usage: "The clustering output file path",
		},
		cli.StringFlag{
			Name:  "media-path",
			Value: "",
			Usage: "The path to the folder containing the media subfolder that is accessible for clustering",
		},
		cli.StringFlag{
			Name:  "output-schema",
			Value: "",
			Usage: "The path to use as output for the clustered schema document",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
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
		schemaPath := c.String("schema")
		output := c.String("output")
		hasHeader := c.Bool("has-header")
		rootDataPath := path.Dir(datasetPath)

		// initialize client
		log.Infof("Using TA2 interface at `%s` ", endpoint)
		client, err := compute.NewClient(endpoint, true, "distil-ingest", "TA2", 5*60*time.Second, 1000, true, nil)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		step := primitive.NewIngestStep(client)

		// create featurizer
		err = step.Cluster(schemaPath, datasetPath, rootDataPath, output, hasHeader)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Clustered data written to %s", output)

		return nil
	}
	// run app
	app.Run(os.Args)
}
