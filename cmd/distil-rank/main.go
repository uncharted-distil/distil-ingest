package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
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
			Name:  "ranking-output",
			Value: "",
			Usage: "The numeric output file path to use for for the file to rank",
		},
		cli.IntFlag{
			Name:  "row-limit",
			Value: 1000,
			Usage: "The number of rows to send to the ranking system",
		},
	}
	app.Action = func(c *cli.Context) error {

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
		if c.String("ranking-output") == "" {
			return cli.NewExitError("missing commandline flag `--ranking-output`", 1)
		}

		classificationPath := filepath.Clean(c.String("classification"))
		typeSource := c.String("type-source")
		schemaPath := filepath.Clean(c.String("schema"))
		rankingFunction := c.String("ranking-function")
		restBaseEndpoint := c.String("rest-endpoint")
		datasetPath := filepath.Clean(c.String("dataset"))
		rankingOutputFile := c.String("ranking-output")
		rowLimit := c.Int("row-limit")
		hasHeader := c.Bool("has-header")

		outputFilePath := c.String("output")

		var err error

		// load the metadata
		var meta *metadata.Metadata
		if schemaPath == "" {
			log.Infof("Loading metadata from raw file")
			meta, err = metadata.LoadMetadataFromRawFile(classificationPath, datasetPath)
		}
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
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// get header for the merged data
		headers, err := meta.GenerateHeaders()
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// merged data only has 1 header
		header := headers[0]

		// add the header to the raw data
		data, err := getMergedData(header, datasetPath, hasHeader, rowLimit)

		// write to file to submit the file
		err = ioutil.WriteFile(rankingOutputFile, data, 0644)
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
		log.Infof("Getting importance ranking of file `%s`", rankingOutputFile)
		importance, err := ranker.RankFile(rankingOutputFile)
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

func getMergedData(header []string, datasetPath string, hasHeader bool, rowLimit int) ([]byte, error) {
	// Copy source to destination.
	file, err := os.Open(datasetPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open source file")
	}

	reader := csv.NewReader(file)

	// output writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)
	if header != nil && len(header) > 0 {
		err := writer.Write(header)
		if err != nil {
			return nil, errors.Wrap(err, "failed to write header to file")
		}
	}

	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "failed to read line from file")
		}
		if (count > 0 || !hasHeader) && count < rowLimit {
			err := writer.Write(line)
			if err != nil {
				return nil, errors.Wrap(err, "failed to write line to file")
			}
		}
		count++
	}
	// flush writer
	writer.Flush()

	// close left
	err = file.Close()
	if err != nil {
		return nil, errors.Wrap(err, "failed to close input file")
	}
	return output.Bytes(), nil
}
