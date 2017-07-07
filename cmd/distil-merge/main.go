package main

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/merge"
)

const (
	d3mIndexColName = "d3mIndex"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-merge"
	app.Version = "0.1.0"
	app.Usage = "Merge D3M training datasets"
	app.UsageText = "distil-merge --schema=<filepath> --training-data=<filepath> --training-targets=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The dataset schema file path",
		},
		cli.StringFlag{
			Name:  "training-data",
			Value: "",
			Usage: "The training data file path",
		},
		cli.StringFlag{
			Name:  "training-targets",
			Value: "",
			Usage: "The training targets file path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The merged output file path",
		},
	}
	app.Action = func(c *cli.Context) error {

		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("training-data") == "" {
			return cli.NewExitError("missing commandline flag `--training-data`", 1)
		}
		if c.String("training-targets") == "" {
			return cli.NewExitError("missing commandline flag `--training-targets`", 1)
		}
		if c.String("output") == "" {
			return cli.NewExitError("missing commandline flag `--output`", 1)
		}
		schemaPath := filepath.Clean(c.String("schema"))
		trainingDataPath := filepath.Clean(c.String("training-data"))
		trainingTargetsPath := filepath.Clean(c.String("training-targets"))
		outputFilePath := filepath.Clean(c.String("output"))

		// Merge targets into training data before ingest
		indices, err := merge.GetColIndices(schemaPath, d3mIndexColName)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 1)
		}

		log.Infof("Joining %s and %s on column indices %d and %d",
			trainingDataPath,
			trainingTargetsPath,
			indices.LeftColIdx,
			indices.RightColIdx)

		success, failed, err := merge.LeftJoin(
			trainingDataPath, indices.LeftColIdx,
			trainingTargetsPath, indices.RightColIdx,
			outputFilePath, true)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		if failed == 0 {
			log.Infof("Merged %d lines successfully, written to %s", success, outputFilePath)
		} else {
			log.Warnf("Merged %d lines, %d lines unmatched, written to %s", success, failed, outputFilePath)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
