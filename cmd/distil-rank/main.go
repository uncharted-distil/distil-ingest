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
	"github.com/unchartedsoftware/distil-ingest/split"
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
	app.Name = "distil-rank"
	app.Version = "0.1.0"
	app.Usage = "Rank D3M merged datasets"
	app.UsageText = "distil-rank --kafka-endpoints=<urls> --dataset=<filepath> --output=<filepath>"
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
			Name:  "type-source",
			Value: "schema",
			Usage: "The source for the type information, either `schema` or `classification`",
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
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
		cli.StringFlag{
			Name:  "rest-endpoint",
			Value: "",
			Usage: "The REST endpoint url",
		},
		cli.StringFlag{
			Name:  "ranking-function",
			Value: "",
			Usage: "The ranking function to use",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The ranking output file path",
		},
		cli.StringFlag{
			Name:  "numeric-output",
			Value: "",
			Usage: "The numeric output file path to use for numeric variables to rank",
		},
	}
	app.Action = func(c *cli.Context) error {

		if c.String("schema") == "" {
			return cli.NewExitError("missing commandline flag `--schema`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("classification") == "" {
			return cli.NewExitError("missing commandline flag `--classification`", 1)
		}
		if c.String("rest-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("ranking-function") == "" {
			return cli.NewExitError("missing commandline flag `--ranking-function`", 1)
		}
		if c.String("numeric-output") == "" {
			return cli.NewExitError("missing commandline flag `--numeric-output`", 1)
		}

		classificationPath := filepath.Clean(c.String("classification"))
		typeSource := c.String("type-source")
		schemaPath := filepath.Clean(c.String("schema"))
		rankingFunction := c.String("ranking-function")
		restBaseEndpoint := c.String("rest-endpoint")
		datasetPath := filepath.Clean(c.String("dataset"))
		numericOutputFile := c.String("numeric-output")
		hasHeader := c.Bool("has-header")
		includeRaw := c.Bool("include-raw-dataset")

		outputFilePath := c.String("output")

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

		// load the metadata
		var meta *metadata.Metadata
		if typeSource == "classification" {
			log.Infof("Loading metadata from classification file")
			meta, err = metadata.LoadMetadataFromClassification(
				schemaPath,
				classificationPath)
		} else {
			log.Infof("Loading metadata from schema file")
			meta, err = metadata.LoadMetadataFromMergedSchema(
				schemaPath)
		}

		// split numeric columns
		log.Infof("Splitting out numeric columns from %s for ranking and writing to %s", datasetPath, numericOutputFile)
		output, err := split.GetNumericColumns(
			datasetPath,
			meta,
			hasHeader)

		// write to file to submit the file
		err = ioutil.WriteFile(numericOutputFile, output, 0644)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// create the REST client
		log.Infof("Using REST interface at `%s/%s` ", restBaseEndpoint, rankingFunction)
		client := rest.NewClient(restBaseEndpoint)

		// create ranker
		ranker := rest.NewRanker(rankingFunction, client)

		// get the importance from the REST interface
		log.Infof("Getting importance ranking of file `%s`", numericOutputFile)
		importance, err := ranker.RankFile(numericOutputFile)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// marshall result
		bytes, err := json.MarshalIndent(importance, "", "    ")
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		// write to file
		log.Infof("Writing importance ranking to file `%s`", outputFilePath)
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
