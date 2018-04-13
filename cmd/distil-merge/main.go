package main

import (
	"bytes"
	"encoding/csv"
	"io"
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

		if c.String("data") == "" {
			return cli.NewExitError("missing commandline flag `--data`", 1)
		}
		if c.String("output-path") == "" {
			return cli.NewExitError("missing commandline flag `--output-path`", 1)
		}

		outputPath := filepath.Clean(c.String("output-path"))
		dataPath := filepath.Clean(c.String("data"))

		// If no schema provided, assume it is a raw data file.
		if c.String("schema") == "" {
			log.Infof("Schema file not specified so assuming raw dataset being merged")
			return mergeRawData(dataPath, outputPath)
		}

		if c.String("output-schema-path") == "" {
			return cli.NewExitError("missing commandline flag `--output-schema-path`", 1)
		}
		schemaPath := filepath.Clean(c.String("schema"))
		rawDataPath := filepath.Clean(c.String("raw-data"))
		outputBucket := c.String("output-bucket")
		outputKey := c.String("output-key")
		outputPathHeader := filepath.Clean(c.String("output-path-header"))
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

		// get header for the merged data
		headers, err := meta.GenerateHeaders()
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// merged data only has 1 header
		header := headers[0]

		// add the header to the raw data
		data, err := getMergedData(header, outputPath, hasHeader)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// write to file to submit the file
		err = ioutil.WriteFile(outputPathHeader, data, 0644)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// log success / failure
		log.Infof("Merged data with header successfully written to %s", outputPathHeader)

		return nil
	}
	// run app
	app.Run(os.Args)
}

func getMergedData(header []string, datasetPath string, hasHeader bool) ([]byte, error) {
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
		if count > 0 || !hasHeader {
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

func mergeRawData(dataPath string, outputPath string) error {
	in, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	err = out.Sync()
	return err
}
