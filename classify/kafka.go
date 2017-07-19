package classify

import (
	"encoding/json"
	"io"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/pkg/errors"
)

const (
	// EarliestOffset will consume all data ever put on the queue
	EarliestOffset ConsumerOffset = "earliest"
	// LatestOffset will consume only the newest information on the queue.
	LatestOffset ConsumerOffset = "latest"
)

// ConsumerOffset represents the offset for the consumer.
type ConsumerOffset string

// Client represents an kafka client.
type Client struct {
	endpoints []string
	user      string
	broker    *kafka.Broker
}

// Consumer represents a kafka consumer.
type Consumer struct {
	topic         string
	numPartitions int
	consumerIndex int
	consumers     []kafka.Consumer
}

// Producer represents a kafka producer.
type Producer struct {
	producer kafka.Producer
}

// Message represents a kafka message.
type Message struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	FileType string `json:"filetype"`
}

// Result represents a kafka classification result.
type Result struct {
	ID       string                 `json:"id"`
	Status   string                 `json:"status"`
	Samples  map[string]interface{} `json:"samples"`
	Labels   map[string]interface{} `json:"labels"`
	Path     string                 `json:"path"`
	FileType string                 `json:"filetype"`
	Raw      string                 `json:"-"`
}

// NewClient instantiates and returns a new kafka client.
func NewClient(endpoints []string, user string) (*Client, error) {
	// instantiate client
	client := &Client{
		endpoints: endpoints,
		user:      user,
	}
	// create broker config
	conf := kafka.NewBrokerConf(client.user)
	// connect to kafka cluster
	broker, err := kafka.Dial(client.endpoints, conf)
	if err != nil {
		return nil, err
	}
	client.broker = broker
	return client, nil
}

// NewConsumer returns a new consumer object for the provided topic.
func (c *Client) NewConsumer(topic string, offset ConsumerOffset) (*Consumer, error) {
	consumer := &Consumer{}
	// get number of partitions
	numPartitions, err := c.broker.PartitionCount(topic)
	if err != nil {
		return nil, err
	}
	consumer.numPartitions = int(numPartitions)
	// create consumer for each partition
	for i := 0; i < consumer.numPartitions; i++ {
		// create consumer config
		conf := kafka.NewConsumerConf(topic, int32(i))
		if offset != EarliestOffset {
			offset, err := c.broker.OffsetLatest(topic, int32(i))
			if err != nil {
				return nil, err
			}
			conf.StartOffset = offset
		}
		// create consumer
		cons, err := c.broker.Consumer(conf)
		if err != nil {
			return nil, err
		}
		consumer.consumers = append(consumer.consumers, cons)
	}
	return consumer, nil
}

// NewProducer returns a new producer object for the provided topic.
func (c *Client) NewProducer() (*Producer, error) {
	producer := &Producer{
		producer: c.broker.Producer(kafka.NewProducerConf()),
	}
	return producer, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.broker.Close()
	return nil
}

// Consume consumes and returns the next portion of the topic.
func (c *Consumer) Consume() (*Result, error) {
	msg, err := c.consumers[c.consumerIndex].Consume()
	if err != nil {
		if err == kafka.ErrNoData {
			return nil, io.EOF
		}
		return nil, err
	}
	c.consumerIndex += c.consumerIndex % c.numPartitions
	// unmarhsal into result
	res := &Result{}
	err = json.Unmarshal(msg.Value, &res)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal json")
	}
	res.Raw = string(msg.Value)
	return res, nil
}

// Produce produces and returns the next portion of the topic.
func (p *Producer) Produce(topic string, partition int32, msg *Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = p.producer.Produce(topic, partition, &proto.Message{
		Value: bytes,
	})
	if err != nil {
		return err
	}
	return nil
}
