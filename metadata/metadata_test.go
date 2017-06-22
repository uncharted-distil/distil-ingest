package metadata

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"io/ioutil"

	"github.com/jeffail/gabs"
	"github.com/stretchr/testify/assert"
	elastic "gopkg.in/olivere/elastic.v5"
)

func TestIngestMetadata(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testString, err := ioutil.ReadFile("testdata/dataDescription.txt")
		assert.NoError(t, err)

		reqBody, err := gabs.ParseJSONBuffer(r.Body)
		println(reqBody.String())
		assert.NoError(t, err)
		assert.Equal(t, reqBody.Path("name").Data().(string), "test dataset")
		assert.Equal(t, reqBody.Path("datasetId").Data().(string), "test_dataset")
		assert.Equal(t, reqBody.Path("description").Data().(string), string(testString))

		variables, err := reqBody.Path("variables").Children()
		assert.NoError(t, err)
		assert.Equal(t, 2, len(variables))
		assert.Equal(t, "alpha", variables[0].Path("varName").Data().(string))
		assert.Equal(t, "attribute", variables[0].Path("varRole").Data().(string))
		assert.Equal(t, "text", variables[0].Path("varType").Data().(string))
		assert.Equal(t, "whiskey", variables[1].Path("varName").Data().(string))
		assert.Equal(t, "target", variables[1].Path("varRole").Data().(string))
		assert.Equal(t, "integer", variables[1].Path("varType").Data().(string))

		_, err = w.Write([]byte(`{
				"_index":"test_index",
				"_type":"metadata",
				"_id":"test_dataset",
				"_version":1,
				"_shards": {
					"total":2,
					"successful":1,
					"failed":0
				},
				"created":true
			}`))
		assert.NoError(t, err)
	}))

	client, err := elastic.NewSimpleClient(elastic.SetURL(testServer.URL))
	assert.NoError(t, err)

	err = IngestMetadata("test_index", "./testdata/dataSchema.json", client)
	assert.NoError(t, err)
}
