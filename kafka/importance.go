package kafka

import (
	"encoding/json"
	"io"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/pkg/errors"
)

// ImportanceMessage represents a kafka classification message.
type ImportanceMessage struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	FileType string `json:"filetype"`
}

// ImportanceResult represents a kafka classification result.
type ImportanceResult struct {
	ID       string                 `json:"id"`
	Status   string                 `json:"status"`
	Features map[string]interface{} `json:"features"`
	Path     string                 `json:"path"`
	FileType string                 `json:"filetype"`
	Raw      string                 `json:"-"`
}

// ConsumeImportance consumes and returns the next portion of the topic.
func (c *Consumer) ConsumeImportance() (*ImportanceResult, error) {
	msg, err := c.consumers[c.consumerIndex].Consume()
	if err != nil {
		if err == kafka.ErrNoData {
			return nil, io.EOF
		}
		return nil, err
	}
	c.consumerIndex += c.consumerIndex % c.numPartitions
	// unmarhsal into result
	res := &ImportanceResult{}
	err = json.Unmarshal(msg.Value, &res)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal json")
	}
	res.Raw = string(msg.Value)
	return res, nil
}

// ProduceImportance produces and returns the next portion of the topic.
func (p *Producer) ProduceImportance(topic string, partition int32, msg *ImportanceMessage) error {
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
