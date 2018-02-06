package smmry

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
)

const (
	summaryLength = 256
)

func getSummaryFallback(str string) string {
	if len(str) < summaryLength {
		return str
	}
	return str[:summaryLength] + "..."
}

func summaryAPICall(str string, lines int, apiKey string) ([]byte, error) {
	// form args
	form := url.Values{}
	form.Add("sm_api_input", str)
	// url
	url := fmt.Sprintf("http://api.smmry.com/&SM_API_KEY=%s&SM_LENGTH=%d", apiKey, lines)
	// post req
	req, err := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// client
	client := &http.Client{}
	// send it
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "Summary request failed")
	}
	defer resp.Body.Close()
	// parse response body
	return ioutil.ReadAll(resp.Body)
}

// GetSummary hits the smmry API and returns the summarized result.
func GetSummary(str string) (string, error) {
	// load api key
	key := os.Getenv("SMMRY_API_KEY")
	if key == "" {
		return "", errors.New("SMMRY api key is missing from env var `SMMRY_API_KEY`")
	}
	// send summary API call
	body, err := summaryAPICall(str, 5, key)
	if err != nil {
		return "", errors.Wrap(err, "failed reading summary body")
	}
	// parse response
	container, err := gabs.ParseJSON(body)
	if err != nil {
		return "", errors.Wrap(err, "failed parsing summary body as JSON")
	}
	// check for API error
	if container.Path("sm_api_error").Data() != nil {
		// error message
		//errStr := container.Path("sm_api_message").Data().(string)
		// fallback to description
		return getSummaryFallback(str), nil
	}
	summary, ok := container.Path("sm_api_content").Data().(string)
	if !ok {
		return getSummaryFallback(str), nil
	}
	return summary, nil
}

// GetSummaryFromDescription builds the summary from the description.
func GetSummaryFromDescription(description string) (string, error) {
	return getSummaryFallback(description), nil
}
