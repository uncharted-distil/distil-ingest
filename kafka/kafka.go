package kafka

import (
	"github.com/optiopay/kafka"
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
