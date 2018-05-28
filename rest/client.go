package rest

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"

	"github.com/pkg/errors"
)

// Client represents a basic REST client.
type Client struct {
	baseEndpoint string
}

// NewClient instantiates a REST client.
func NewClient(baseEndpoint string) *Client {
	return &Client{
		baseEndpoint: baseEndpoint,
	}
}

// PostFile submits a file in a POST request using a multipart form.
func (c *Client) PostFile(function string, filename string, params map[string]string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", c.baseEndpoint, function)

	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	// add the file
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read file")
	}
	defer f.Close()

	fw, err := w.CreateFormFile("file", filename)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create form request")
	}
	if _, err = io.Copy(fw, f); err != nil {
		return nil, errors.Wrap(err, "Unable to copy file")
	}

	// add the parameters
	for name, value := range params {
		err := w.WriteField(name, value)
		if err != nil {
			return nil, errors.Wrap(err, "unable to add parameter field")
		}
	}
	w.Close()

	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create request")
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to post request")
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", res.Status)
	}

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read result")
	}

	return result, nil
}

// PostRequest submits a post request with the provided parameters.
func (c *Client) PostRequest(function string, params map[string]string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", c.baseEndpoint, function)

	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	// add the parameters
	for name, value := range params {
		err := w.WriteField(name, value)
		if err != nil {
			return nil, errors.Wrap(err, "unable to add parameter field")
		}
	}
	w.Close()

	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create request")
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to post request")
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", res.Status)
	}

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read result")
	}

	return result, nil
}
