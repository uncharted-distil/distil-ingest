package main

import (
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/feature"
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
	app.Name = "distil-featurize"
	app.Version = "0.1.0"
	app.Usage = "Featurize D3M datasets"
	app.UsageText = "distil-featurize --rest-endpoint=<url> --featurize-function=<function> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "rest-endpoint",
			Value: "",
			Usage: "The REST endpoint url",
		},
		cli.StringFlag{
			Name:  "featurize-function",
			Value: "",
			Usage: "The featurize function to use",
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
			Usage: "The featurize output file path",
		},
		cli.StringFlag{
			Name:  "media-path",
			Value: "",
			Usage: "The path to the folder containing the media subfolder that is accessible for featurization",
		},
		cli.StringFlag{
			Name:  "output-schema",
			Value: "",
			Usage: "The path to use as output for the featurized schema document",
		},
		cli.StringFlag{
			Name:  "output-data",
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
		if c.String("rest-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("featurize-function") == "" {
			return cli.NewExitError("missing commandline flag `--featurize-function`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		featurizeFunction := c.String("featurize-function")
		restBaseEndpoint := c.String("rest-endpoint")
		datasetPath := c.String("dataset")
		mediaPath := c.String("media-path")
		outputSchema := c.String("output-schema")
		outputData := c.String("output-data")
		schemaPath := c.String("schema")
		outputFilePath := c.String("output")
		hasHeader := c.Bool("has-header")
		threshold := c.Float64("threshold")

		// initialize REST client
		log.Infof("Using REST interface at `%s` ", restBaseEndpoint)
		client := rest.NewClient(restBaseEndpoint)

		// create feature folder
		featurePath := path.Join(outputFilePath, "features")
		if dirExists(featurePath) {
			// delete existing data to overwrite with latest
			os.RemoveAll(featurePath)
			log.Infof("Deleted data at %s", featurePath)
		}
		if err := os.MkdirAll(featurePath, 0777); err != nil && !os.IsExist(err) {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		os.Remove(path.Join(outputFilePath, "featureDatasetDoc.json"))

		// create featurizer
		featurizer := rest.NewFeaturizer(featurizeFunction, client)

		// load metadata from original schema
		meta, err := metadata.LoadMetadataFromOriginalSchema(schemaPath)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// featurize data
		err = feature.FeaturizeDataset(meta, featurizer, datasetPath, mediaPath, outputFilePath, outputData, outputSchema, hasHeader, threshold)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		log.Infof("Featurized data written to %s", outputFilePath)

		return nil
	}
	// run app
	app.Run(os.Args)
}

func dirExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
