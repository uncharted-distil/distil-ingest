package d3mdata

import (
	"encoding/json"
	"testing"

	"github.com/jeffail/gabs"
	"github.com/stretchr/testify/assert"
	log "github.com/unchartedsoftware/plog"
)

func TestGetSource(t *testing.T) {
	// Create a document using the test json schema
	doc, err := NewD3MData("testdata/dataSchema.json")()
	if err != nil {
		assert.Fail(t, "Failed to create document")
	}

	data := "0,cat_1,99.0,66,ord_1,234324,some text value,podunk indiana,un_1"
	doc.SetData(data)

	// Fetch the doc source
	output, err := doc.GetSource()
	if err != nil {
		log.Error(err)
		assert.Fail(t, "Failed to create document")
	}

	type Record struct {
		Alpha   string
		Bravo   float64
		Charlie int64
		Delta   string
		Echo    int64
		Foxtrot string
		Gamma   string
		Hotel   string
	}

	// Extract it from JSON
	var r Record
	err = json.Unmarshal([]byte(output.(string)), &r)
	if err != nil {
		log.Error(err)
		assert.Fail(t, "Failed to create document")
	}

	assert.Equal(t, "cat_1", r.Alpha)
	assert.Equal(t, 99.0, r.Bravo)
	assert.Equal(t, int64(66), r.Charlie)
	assert.Equal(t, "ord_1", r.Delta)
	assert.Equal(t, int64(234324), r.Echo)
	assert.Equal(t, "some text value", r.Foxtrot)
	assert.Equal(t, "podunk indiana", r.Gamma)
	assert.Equal(t, "un_1", r.Hotel)
}

func TestGetMapping(t *testing.T) {
	// Create a document using the test json schema
	doc, err := NewD3MData("testdata/dataSchema.json")()
	if err != nil {
		assert.Fail(t, "Failed to create document")
	}

	// Fetch the mappings
	strMapping, err := doc.GetMapping()
	if err != nil {
		log.Error(err)
		assert.Fail(t, "Failed to create document")
	}

	mapping, err := gabs.ParseJSON([]byte(strMapping))
	assert.Equal(t, "text", mapping.Path("datum.properties.Alpha.type").Data().(string))
	assert.Equal(t, "double", mapping.Path("datum.properties.Bravo.type").Data().(string))
	assert.Equal(t, "long", mapping.Path("datum.properties.Charlie.type").Data().(string))
	assert.Equal(t, "text", mapping.Path("datum.properties.Delta.type").Data().(string))
	assert.Equal(t, "date", mapping.Path("datum.properties.Echo.type").Data().(string))
	assert.Equal(t, "text", mapping.Path("datum.properties.Foxtrot.type").Data().(string))
	assert.Equal(t, "text", mapping.Path("datum.properties.Golf.type").Data().(string))
	assert.Equal(t, "text", mapping.Path("datum.properties.Hotel.type").Data().(string))

}

func TestID(t *testing.T) {
	// Create a document using the test json schema
	doc, err := NewD3MData("testdata/dataSchema.json")()
	if err != nil {
		assert.Fail(t, "Failed to create document")
	}

	data := "0,cat_1,99.0,66,ord_1,234324,some text value,podunk indiana,un_1"
	doc.SetData(data)

	// Fetch id
	id, err := doc.GetID()
	if err != nil {
		log.Error(err)
		assert.Fail(t, "Failed to create document")
	}

	// Verify the id
	assert.Equal(t, "0", id)
}
