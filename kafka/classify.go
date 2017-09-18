package kafka

import (
	"encoding/json"
	"io"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/pkg/errors"
)

// ClassificationMessage represents a kafka classification message.
type ClassificationMessage struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	FileType string `json:"filetype"`
}

// ClassificationResult represents a kafka classification result.
type ClassificationResult struct {
	ID       string                 `json:"id"`
	Status   string                 `json:"status"`
	Samples  map[string]interface{} `json:"samples"`
	Labels   map[string]interface{} `json:"labels"`
	Path     string                 `json:"path"`
	FileType string                 `json:"filetype"`
	Raw      string                 `json:"-"`
}

// ConsumeClassification consumes and returns the next portion of the topic.
func (c *Consumer) ConsumeClassification() (*ClassificationResult, error) {
	msg, err := c.consumers[c.consumerIndex].Consume()
	if err != nil {
		if err == kafka.ErrNoData {
			return nil, io.EOF
		}
		return nil, err
	}
	c.consumerIndex += c.consumerIndex % c.numPartitions
	// unmarhsal into result
	res := &ClassificationResult{}
	err = json.Unmarshal(msg.Value, &res)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal json")
	}
	res.Raw = string(msg.Value)
	return res, nil
}

// ProduceClassification produces and returns the next portion of the topic.
func (p *Producer) ProduceClassification(topic string, partition int32, msg *ClassificationMessage) error {
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
