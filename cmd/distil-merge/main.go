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
	app.UsageText = "distil-merge --schema=<filepath> --data=<filepath> --output-path=<filepath> --output-schema-path=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The dataset schema file path",
		},
		cli.StringFlag{
			Name:  "data",
			Value: "",
			Usage: "The data file path",
		},
		cli.StringFlag{
			Name:  "raw-data",
			Value: "",
			Usage: "The raw dat a file path",
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

		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("data") == "" {
			return cli.NewExitError("missing commandline flag `--data`", 1)
		}
		if c.String("output-path") == "" {
			return cli.NewExitError("missing commandline flag `--output-path`", 1)
		}
		if c.String("output-schema-path") == "" {
			return cli.NewExitError("missing commandline flag `--output-schema-path`", 1)
		}
		schemaPath := filepath.Clean(c.String("schema"))
		dataPath := filepath.Clean(c.String("data"))
		rawDataPath := filepath.Clean(c.String("raw-data"))
		outputBucket := c.String("output-bucket")
		outputKey := c.String("output-key")
		outputPath := filepath.Clean(c.String("output-path"))
		outputSchemaPath := filepath.Clean(c.String("output-schema-path"))
		hasHeader := c.Bool("has-header")

		// load the metadata from schema
		meta, err := metadata.LoadMetadataFromOriginalSchema(schemaPath)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 1)
		}

		// merge file links in dataset
		mergedDR, output, err := merge.InjectFileLinksFromFile(meta, dataPath, rawDataPath, hasHeader)
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
		if outputBucket != "" {
			err = s3.WriteToBucket(client, outputBucket, outputKey, output)
			if err != nil {
				log.Errorf("%+v", err)
				return cli.NewExitError(errors.Cause(err), 4)
			}
		}

		// write copy to disk
		err = ioutil.WriteFile(outputPath, output, 0644)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 5)
		}

		// write merged metadata out to disk
		err = meta.WriteMergedSchema(outputSchemaPath, mergedDR)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 5)
		}

		// log success / failure
		log.Infof("Merged data successfully written to %s", outputPath)
		if outputBucket != "" {
			log.Infof("Merged data successfully written to %s/%s", outputBucket, outputKey)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
