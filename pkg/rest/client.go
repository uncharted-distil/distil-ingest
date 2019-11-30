//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package rest

import (
	"bytes"
	"encoding/json"
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

// PostRequestRaw submits a post request with the provided parameters
// submitted as a raw string.
func (c *Client) PostRequestRaw(function string, params map[string]interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", c.baseEndpoint, function)
	b, err := json.Marshal(params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal parameters")
	}

	// interface requires double marshalling to have a raw string
	b, err = json.Marshal(string(b))
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal (*2) parameters")
	}

	res, err := http.Post(url, "application/json", bytes.NewReader(b))
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
