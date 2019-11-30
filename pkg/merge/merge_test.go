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
