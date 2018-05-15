package metadata

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jeffail/gabs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
)

func TestMetadataFromSchema(t *testing.T) {

	meta, err := LoadMetadataFromOriginalSchema("./testdata/datasetDoc.json")
	assert.NoError(t, err)

	assert.Equal(t, meta.Name, "test dataset")
	assert.Equal(t, meta.ID, "test_dataset")
	assert.Equal(t, meta.Description, "YOU ARE STANDING AT THE END OF A ROAD BEFORE A SMALL BRICK BUILDING.")
	assert.Equal(t, len(meta.DataResources[0].Variables), 3)
	assert.Equal(t, meta.DataResources[0].Variables[0].Name, "bravo")
	assert.Equal(t, meta.DataResources[0].Variables[0].Role, []string{"index"})
	assert.Equal(t, meta.DataResources[0].Variables[0].Type, "integer")
	assert.Equal(t, meta.DataResources[0].Variables[0].OriginalType, "integer")
	assert.Equal(t, meta.DataResources[0].Variables[1].Name, "alpha")
	assert.Equal(t, meta.DataResources[0].Variables[1].Role, []string{"attribute"})
	assert.Equal(t, meta.DataResources[0].Variables[1].Type, "text")
	assert.Equal(t, meta.DataResources[0].Variables[1].OriginalType, "text")
	assert.Equal(t, meta.DataResources[0].Variables[2].Name, "whiskey")
	assert.Equal(t, meta.DataResources[0].Variables[2].Role, []string{"suggestedTarget"})
	assert.Equal(t, meta.DataResources[0].Variables[2].Type, "integer")
	assert.Equal(t, meta.DataResources[0].Variables[2].OriginalType, "integer")
}

func TestIngestMetadata(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := gabs.ParseJSONBuffer(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, reqBody.Path("datasetName").Data().(string), "test dataset")
		assert.Equal(t, reqBody.Path("datasetID").Data().(string), "test_dataset")
		assert.Equal(t, reqBody.Path("description").Data().(string), string("YOU ARE STANDING AT THE END OF A ROAD BEFORE A SMALL BRICK BUILDING."))

		variables, err := reqBody.Path("variables").Children()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(variables))
		assert.Equal(t, "bravo", variables[0].Path("colName").Data().(string))
		roles := variables[0].Path("role").Data().([]interface{})
		assert.Equal(t, "index", roles[0].(string))
		assert.Equal(t, "integer", variables[0].Path("colType").Data().(string))
		assert.Equal(t, "integer", variables[0].Path("colOriginalType").Data().(string))

		assert.Equal(t, "alpha", variables[1].Path("colName").Data().(string))
		roles = variables[1].Path("role").Data().([]interface{})
		assert.Equal(t, "attribute", roles[0].(string))
		assert.Equal(t, "text", variables[1].Path("colType").Data().(string))
		assert.Equal(t, "text", variables[1].Path("colOriginalType").Data().(string))

		assert.Equal(t, "whiskey", variables[2].Path("colName").Data().(string))
		roles = variables[2].Path("role").Data().([]interface{})
		assert.Equal(t, "suggestedTarget", roles[0].(string))
		assert.Equal(t, "integer", variables[2].Path("colType").Data().(string))
		assert.Equal(t, "integer", variables[2].Path("colOriginalType").Data().(string))

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

	meta, err := LoadMetadataFromOriginalSchema("./testdata/datasetDoc.json")
	assert.NoError(t, err)

	err = IngestMetadata(client, "test_index", "", meta)
	assert.NoError(t, err)
}
