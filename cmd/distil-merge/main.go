package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/merge"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/s3"
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
		cli.BoolFlag{
			Name:  "include-raw-dataset",
			Usage: "If true, will process raw datasets",
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
			Name:  "output-bucket",
			Value: "",
			Usage: "The merged output AWS S3 bucket",
		},
		cli.StringFlag{
			Name:  "output-key",
			Value: "",
			Usage: "The merged output AWS S3 key",
		},
		cli.StringFlag{
			Name:  "output-path",
			Value: "",
			Usage: "The merged output path",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
		cli.BoolFlag{
			Name:  "include-header",
			Usage: "Whether or not to include the header row in the merged file",
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
		if c.String("output-bucket") == "" {
			return cli.NewExitError("missing commandline flag `--output-bucket`", 1)
		}
		if c.String("output-key") == "" {
			return cli.NewExitError("missing commandline flag `--output-key`", 1)
		}
		if c.String("output-path") == "" {
			return cli.NewExitError("missing commandline flag `--output-path`", 1)
		}
		schemaPath := filepath.Clean(c.String("schema"))
		trainingDataPath := filepath.Clean(c.String("training-data"))
		trainingTargetsPath := filepath.Clean(c.String("training-targets"))
		outputBucket := c.String("output-bucket")
		outputKey := c.String("output-key")
		outputPath := filepath.Clean(c.String("output-path"))
		hasHeader := c.Bool("has-header")
		includeHeader := c.Bool("include-header")
		includeRaw := c.Bool("include-raw-dataset")

		// Check if it is a raw dataset
		isRaw, err := metadata.IsRawDataset(schemaPath)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 1)
		}
		if isRaw && !includeRaw {
			log.Infof("Not processing dataset because it is a raw dataset")
			return nil
		}

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

		output, success, failed, err := merge.LeftJoin(
			trainingDataPath,
			indices.LeftColIdx,
			trainingTargetsPath,
			indices.RightColIdx,
			hasHeader,
			includeHeader)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// get AWS S3 client
		client, err := s3.NewClient()
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 3)
		}

		// write merged output to AWS S3
		err = s3.WriteToBucket(client, outputBucket, outputKey, output)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 4)
		}

		// write copy to disk
		err = ioutil.WriteFile(outputPath, output, 0644)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 5)
		}

		// log success / failure
		if failed == 0 {
			log.Infof("Merged %d lines successfully, written to %s/%s", success, outputBucket, outputKey)
		} else {
			log.Warnf("Merged %d lines, %d lines unmatched, written to %s/%s", success, failed, outputBucket, outputKey)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
