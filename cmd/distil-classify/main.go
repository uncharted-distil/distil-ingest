package main

import (
	"io"
	"os"
	"runtime"

	"github.com/pkg/errors"
	//"github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/classify"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-classify"
	app.Version = "0.1.0"
	app.Usage = "Classify D3M training datasets"
	app.UsageText = "distil-classify --schema=<filepath> --training-data=<filepath> --training-targets=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
	// cli.StringFlag{
	// 	Name:  "schema",
	// 	Value: "",
	// 	Usage: "The dataset schema file path",
	// },
	// cli.StringFlag{
	// 	Name:  "training-data",
	// 	Value: "",
	// 	Usage: "The training data file path",
	// },
	// cli.StringFlag{
	// 	Name:  "training-targets",
	// 	Value: "",
	// 	Usage: "The training targets file path",
	// },
	// cli.StringFlag{
	// 	Name:  "output",
	// 	Value: "",
	// 	Usage: "The merged output file path",
	// },
	}
	app.Action = func(c *cli.Context) error {

		/*

			We will use Kafka as a sort of message bus for communicating between NK
			and Uncharted. I'm open to alternative options, such as RabbitMQ, but I
			like Kafka because it gives us the option to directly load large
			messages (such as data files) onto the message bus, and gives us some
			nice history tracking that isn't available in RabbitMQ. In the short
			term, NK will host the Kafka instance, long term we will run Kafka on
			OpenStack.

			When a data file in HDFS (I will assume .csv for now) needs to have its
			columns classified:
			1. Uncharted publishes a message to the Kafka topic
				"classify_column_datatypes", requesting columns in a file be
				classified. This request, at minimum must contain the following
				information:

					a. Identifier for file
					b. Path to file in HDFS (as well as any necessary credentials)

				e.g.

					{
						"id": "bc5900e9-3588-414a-a0fc-565857b59eb9",
						"path": "hdfs://123.123.123.123:9000/path/and/filename.csv",
						"filetype": "csv"
					}

			2. NK consumes from "classify_column_datatypes", reads the file from
				HDFS, and produces labels

			3. NK publishes a message to the Kafka topic
				"column_datatype_classifications", containing the following
				information:

					a. Identifier for file
					b. key: value pairs for every original column name and the
						label.

				e.g.

					{
						"id": "bc5900e9-3588-414a-a0fc-565857b59eb9",
						"labels":{
							"col1": "int",
							"col2": "money",
							"col3": "unknown"
						}
					}

		*/

		//produceTopic := "classify_column_datatypes"
		consumeTopic := "column_datatype_classifications"

		kafkaURLs := []string{"10.104.2.32:9092"}
		kafkaUser := "uncharted-distil"

		log.Infof("Connecting to kafkfa %v as user %s", kafkaURLs, kafkaUser)
		// connect to kafka
		client, err := classify.NewClient(kafkaURLs, kafkaUser)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		/*
			// create producer
			log.Infof("Creating producer")
			producer, err := client.NewProducer()
			if err != nil {
				log.Errorf("%+v", err)
				return cli.NewExitError(errors.Cause(err), 2)
			}

			id := uuid.NewV4().String()
			path := "https://s3.amazonaws.com/d3m-data/merged_o_data/o_185_merged.csv"
			filetype := "csv"
			log.Info("Classification UUID %s", id)

			log.Infof("Initializing classification for %s, %s on topic %s", path, id, produceTopic)
			err = producer.Produce(produceTopic, 0, &classify.Message{
				ID:       id,
				Path:     path,
				FileType: filetype,
			})
			if err != nil {
				log.Errorf("%+v", err)
				return cli.NewExitError(errors.Cause(err), 2)
			}
		*/

		log.Infof("Consuming classifications on topic %s", consumeTopic)
		consumer, err := client.NewConsumer(consumeTopic)
		if err != nil {
			log.Errorf("%+v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		for {
			res, err := consumer.Consume()
			if err != nil {
				if err == io.EOF {
					log.Infof("Finished consuming")
					break
				}
				log.Errorf("%+v", err)
				return cli.NewExitError(errors.Cause(err), 2)
			}
			log.Infof("Consumed %s", res.FileName)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}
