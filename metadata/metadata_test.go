package metadata

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"io/ioutil"

	"github.com/jeffail/gabs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
)

func TestMetadataFromSchema(t *testing.T) {

	meta, err := LoadMetadataFromOriginalSchema("./testdata/dataSchema.json")
	assert.NoError(t, err)

	description, err := ioutil.ReadFile("./testdata/dataDescription.txt")
	assert.NoError(t, err)

	assert.Equal(t, meta.Name, "test dataset")
	assert.Equal(t, meta.ID, "test_dataset")
	assert.Equal(t, meta.Description, string(description))
	assert.Equal(t, len(meta.Variables), 4)
	assert.Equal(t, meta.Variables[0].Name, "bravo")
	assert.Equal(t, meta.Variables[0].Role, "index")
	assert.Equal(t, meta.Variables[0].Type, "integer")
	assert.Equal(t, meta.Variables[1].Name, "alpha")
	assert.Equal(t, meta.Variables[1].Role, "attribute")
	assert.Equal(t, meta.Variables[1].Type, "text")
	assert.Equal(t, meta.Variables[2].Name, "victor")
	assert.Equal(t, meta.Variables[2].Role, "index")
	assert.Equal(t, meta.Variables[2].Type, "integer")
	assert.Equal(t, meta.Variables[3].Name, "whiskey")
	assert.Equal(t, meta.Variables[3].Role, "target")
	assert.Equal(t, meta.Variables[3].Type, "integer")
}

func TestIngestMetadata(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testString, err := ioutil.ReadFile("./testdata/dataDescription.txt")
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

	meta, err := LoadMetadataFromOriginalSchema("./testdata/dataSchema.json")
	assert.NoError(t, err)

	err = IngestMetadata(client, "test_index", meta)
	assert.NoError(t, err)
}
