package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/kafka"
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
	app.UsageText = "distil-classify --kafka-endpoints=<urls> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "kafka-endpoints",
			Value: "",
			Usage: "The kafka endpoint urls, comma separated",
		},
		cli.StringFlag{
			Name:  "kafka-user",
			Value: "uncharted-distil",
			Usage: "The kafka user",
		},
		cli.StringFlag{
			Name:  "consume-topic",
			Value: "column_datatype_classifications",
			Usage: "The topic to consume a classification",
		},
		cli.StringFlag{
			Name:  "produce-topic",
			Value: "classify_column_datatypes",
			Usage: "The topic to produce a classification",
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

		if c.String("kafka-endpoints") == "" {
			return cli.NewExitError("missing commandline flag `--kafka-endpoints`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		produceTopic := c.String("produce-topic")
		consumeTopic := c.String("consume-topic")
		kafkaURLs := splitAndTrim(c.String("kafka-endpoints"))
		kafkaUser := c.String("kafka-user")
		path := c.String("dataset")
		filetype := c.String("filetype")
		outputFilePath := c.String("output")
		id := "uncharted_" + uuid.NewV4().String()

		// connect to kafka
		log.Infof("Connecting to kafka `%s` as user `%s`", strings.Join(kafkaURLs, ","), kafkaUser)
		client, err := kafka.NewClient(kafkaURLs, kafkaUser)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// create consumer
		consumer, err := client.NewConsumer(consumeTopic, kafka.LatestOffset)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// create producer
		producer, err := client.NewProducer()
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// dispatch classification task
		log.Infof("Initializing classification for `%s` on topic `%s` with id `%s`", path, produceTopic, id)
		err = producer.ProduceClassification(produceTopic, 0, &kafka.ClassificationMessage{
			ID:       id,
			Path:     path,
			FileType: filetype,
		})
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		// consume classification results
		log.Infof("Consuming classification for `%s` on topic `%s`", path, consumeTopic)
		for {
			res, err := consumer.ConsumeClassification()
			if err != nil {
				if err == io.EOF {
					log.Infof("Finished consuming")
					break
				}
				log.Errorf("%v", err)
				break
			}
			if res.ID == id {
				if res.Status == "Failure" {
					return cli.NewExitError(fmt.Sprintf("Classification for `%s` failed", path), 2)
				}
				log.Infof("Classification for `%s` of id `%s` successful", path, id)
				// marshall result
				bytes, err := json.MarshalIndent(res, "", "    ")
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
				break
			}
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
