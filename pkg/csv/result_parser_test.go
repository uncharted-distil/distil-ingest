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

package csv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSVResultParser(t *testing.T) {
	result, err := ParseResultCSV("./testdata/test.csv")
	assert.NoError(t, err)
	assert.NotEmpty(t, result)

	fmt.Printf("%v", result)

	assert.Equal(t, []interface{}{"idx", "col a", "col b"}, result[0])
	assert.Equal(t, []interface{}{"0", []interface{}{"alpha", "bravo"}, "foxtrot"}, result[1])
	assert.Equal(t, []interface{}{"1", []interface{}{"charlie", "delta's oscar"}, "hotel"}, result[2])
	assert.Equal(t, []interface{}{"2", []interface{}{"a", "[", "b"}, []interface{}{"c", "\"", "e"}}, result[3])
	assert.Equal(t, []interface{}{"3", []interface{}{"a", "['\"", "b"}, []interface{}{"c", "\"", "e"}}, result[4])
}
