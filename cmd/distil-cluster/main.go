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
	app.Name = "distil-cluster"
	app.Version = "0.1.0"
	app.Usage = "Cluster D3M datasets"
	app.UsageText = "distil-cluster --rest-endpoint=<url> --cluster-function=<function> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "rest-endpoint",
			Value: "",
			Usage: "The REST endpoint url",
		},
		cli.StringFlag{
			Name:  "cluster-function",
			Value: "",
			Usage: "The clustering function to use",
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
		cli.StringFlag{
			Name:  "output-data",
			Value: "",
			Usage: "The path to use as output for the clustered data",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("rest-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("cluster-function") == "" {
			return cli.NewExitError("missing commandline flag `--cluster-function`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		clusterFunction := c.String("cluster-function")
		restBaseEndpoint := c.String("rest-endpoint")
		datasetPath := c.String("dataset")
		mediaPath := c.String("media-path")
		outputSchema := c.String("output-schema")
		outputData := c.String("output-data")
		schemaPath := c.String("schema")
		outputFilePath := c.String("output")
		hasHeader := c.Bool("has-header")

		// initialize REST client
		log.Infof("Using REST interface at `%s` ", restBaseEndpoint)
		client := rest.NewClient(restBaseEndpoint)

		// create feature folder
		clusterPath := path.Join(outputFilePath, "clusters")
		if dirExists(clusterPath) {
			// delete existing data to overwrite with latest
			os.RemoveAll(clusterPath)
			log.Infof("Deleted data at %s", clusterPath)
		}
		if err := os.MkdirAll(clusterPath, 0777); err != nil && !os.IsExist(err) {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		os.Remove(path.Join(outputFilePath, "clusterDatasetDoc.json"))

		// create featurizer
		featurizer := rest.NewFeaturizer(clusterFunction, client)

		// load metadata from original schema
		meta, err := metadata.LoadMetadataFromOriginalSchema(schemaPath)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// featurize data
		err = feature.ClusterDataset(meta, featurizer, datasetPath, mediaPath, outputFilePath, outputData, outputSchema, hasHeader)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		log.Infof("Clustered data written to %s", outputFilePath)

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
