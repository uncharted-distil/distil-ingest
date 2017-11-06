package merge

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetD3MIndices(t *testing.T) {
	indices, err := GetColIndices("testdata/dataSchema.json", "d3mIndex")
	assert.NoError(t, err)
	assert.Equal(t, indices.LeftColIdx, 1)
	assert.Equal(t, indices.RightColIdx, 0)
}

func TestLeftJoin(t *testing.T) {
	output, success, failed, err := LeftJoin(
		"testdata/trainData.csv", 1,
		"testdata/trainTargets.csv", 0,
		true)
	assert.NoError(t, err)
	assert.Equal(t, success, 3)
	assert.Equal(t, failed, 0)

	// Create a new Scanner for the file.
	scanner := bufio.NewScanner(bytes.NewReader(output))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	assert.Equal(t, "10,0,100.0,1,1.0", lines[0])
	assert.Equal(t, "20,1,200.0,2,2.0", lines[1])

}
