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
	"runtime"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil/api/env"
	"github.com/uncharted-distil/distil/api/task"
)

func splitAndTrim(arg string) []string {
	var res []string
	if arg == "" {
		return res
	}
	split := strings.Split(arg, ",")
	for _, str := range split {
		res = append(res, strings.TrimSpace(str))
	}
	return res
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-geocode"
	app.Version = "0.1.0"
	app.Usage = "Geocode D3M merged datasets"
	app.UsageText = "distil-geocode --endpoint=<url> --dataset=<filepath> --output=<filepath>"
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
			Name:  "classification",
			Value: "",
			Usage: "The classification source path",
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
			Usage: "The path to use as output for the geocoded data",
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
		outputPath := c.String("output")
		//hasHeader := c.Bool("has-header")
		//classificationPath := filepath.Clean(c.String("classification"))
		//rootDataPath := path.Dir(datasetPath)

		// initialize config
		log.Infof("Using TA2 interface at `%s` ", endpoint)
		config, err := env.LoadConfig()
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		config.GeocodingOutputDataRelative = outputPath
		config.GeocodingOutputSchemaRelative = outputPath
		config.SolutionComputeEndpoint = endpoint

		ingestConfig := task.NewConfig(config)

		// geocode the file
		geocodePath, err := task.GeocodeForwardDataset(metadata.Seed, schemaPath, "", datasetPath, ingestConfig)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Geocoding for `%s` successful", geocodePath)

		return nil
	}
	// run app
	app.Run(os.Args)
}
