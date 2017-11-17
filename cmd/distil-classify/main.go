package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/rest"
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
	app.Name = "distil-classify"
	app.Version = "0.1.0"
	app.Usage = "Classify D3M merged datasets"
	app.UsageText = "distil-classify --rest-endpoint=<url> --classification-function=<function> --dataset=<filepath> --output=<filepath>"
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
			Name:  "rest-endpoint",
			Value: "",
			Usage: "The REST endpoint url",
		},
		cli.StringFlag{
			Name:  "classification-function",
			Value: "",
			Usage: "The classification function to use",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "filetype",
			Value: "csv",
			Usage: "The dataset file type",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The classification output file path",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("rest-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("classification-function") == "" {
			return cli.NewExitError("missing commandline flag `--classification-function`", 1)
		}
		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		schemaPath := filepath.Clean(c.String("schema"))
		classificationFunction := c.String("classification-function")
		restBaseEndpoint := c.String("rest-endpoint")
		path := c.String("dataset")
		outputFilePath := c.String("output")
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

		// initialize REST client
		log.Infof("Using REST interface at `%s` ", restBaseEndpoint)
		client := rest.NewClient(restBaseEndpoint)

		// create classifier
		classifier := rest.NewClassifier(classificationFunction, client)

		// classify the file
		classification, err := classifier.ClassifyFile(path)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Classification for `%s` successful", path)
		// marshall result
		bytes, err := json.MarshalIndent(classification, "", "    ")
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		// write to file
		log.Infof("Writing classification to file `%s`", outputFilePath)
		err = ioutil.WriteFile(outputFilePath, bytes, 0644)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
