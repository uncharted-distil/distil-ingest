package s3

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// NewClient returns a new S3 client using the aws session
func NewClient() (*s3.S3, error) {
	// The session the S3 Uploader will use
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

// WriteToBucket writes the data to the provided bucket
func WriteToBucket(client *s3.S3, bucket string, key string, data []byte) error {
	r := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		ACL:    aws.String("public-read"),
		Body:   aws.ReadSeekCloser(r),
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := client.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}
