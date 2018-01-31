package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/rest"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-summary"
	app.Version = "0.1.0"
	app.Usage = "Summarize D3M datasets"
	app.UsageText = "distil-summary --rest-endpoint=<url> --summary-function=<function> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "rest-endpoint",
			Value: "",
			Usage: "The REST endpoint url",
		},
		cli.StringFlag{
			Name:  "summary-function",
			Value: "",
			Usage: "The summary function to use",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The summary output file path",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("rest-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--rest-endpoint`", 1)
		}
		if c.String("summary-function") == "" {
			return cli.NewExitError("missing commandline flag `--summary-function`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("output") == "" {
			return cli.NewExitError("missing commandline flag `--output`", 1)
		}

		summaryFunction := c.String("summary-function")
		restBaseEndpoint := c.String("rest-endpoint")
		path := c.String("dataset")
		outputFilePath := c.String("output")

		// initialize REST client
		log.Infof("Using REST interface at `%s` ", restBaseEndpoint)
		client := rest.NewClient(restBaseEndpoint)

		// create classifier
		summarizer := rest.NewSummarizer(summaryFunction, client)

		// classify the file
		summary, err := summarizer.SummarizeFile(path)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Summary for `%s` successful", path)
		// marshall result
		bytes, err := json.MarshalIndent(summary, "", "    ")
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		// write to file
		log.Infof("Writing summary to file `%s`", outputFilePath)
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
