package main

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-ingest/primitive"
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
	app.UsageText = "distil-merge --schema=<filepath> --data=<filepath> --output-path=<filepath> --output-schema-path=<filepath>"
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
			Name:  "raw-data",
			Value: "",
			Usage: "The raw dat a file path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The merged output folder",
		},
		cli.StringFlag{
			Name:  "output-path-relative",
			Value: "",
			Usage: "The merged output path relative to the schema output path",
		},
		cli.StringFlag{
			Name:  "output-path-header",
			Value: "",
			Usage: "The merged with header output path",
		},
		cli.StringFlag{
			Name:  "output-schema-path",
			Value: "",
			Usage: "The merged schema path",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
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

		outputFolderPath := filepath.Clean(c.String("output"))
		endpoint := filepath.Clean(c.String("endpoint"))
		dataset := filepath.Clean(c.String("dataset"))

		// initialize client
		log.Infof("Using pipeline runner interface at `%s` ", endpoint)
		client, err := compute.NewRunner(endpoint, true, "distil-ingest", 60, 10, true)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		step := primitive.NewIngestStep(client)

		// merge the dataset into a single file
		err = step.Merge(dataset, outputFolderPath)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Merged data written to %s", outputFolderPath)

		return nil
	}
	// run app
	app.Run(os.Args)
}
