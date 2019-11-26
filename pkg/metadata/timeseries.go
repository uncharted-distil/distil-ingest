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

package metadata

import (
	"fmt"

	"github.com/Jeffail/gabs/v2"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
)

// Timeseries is a data resource that is contained within one or many timeseries files.
type Timeseries struct {
}

// Parse extracts the data resource from the data schema document.
func (r *Timeseries) Parse(res *gabs.Container) (*model.DataResource, error) {
	schemaVariables := res.Path("columns").Children()
	if schemaVariables == nil {
		return nil, errors.New("failed to parse column data")
	}

	if res.Path("resID").Data() == nil {
		return nil, fmt.Errorf("unable to parse resource id")
	}
	resID := res.Path("resID").Data().(string)

	if res.Path("resPath").Data() == nil {
		return nil, fmt.Errorf("unable to parse resource path")
	}
	resPath := res.Path("resPath").Data().(string)

	var resFormats []string
	if res.Path("resFormat").Data() != nil {
		formatsRaw := res.Path("resFormat").Children()
		if formatsRaw == nil {
			return nil, errors.New("unable to parse resource format")
		}
		resFormats = make([]string, len(formatsRaw))
		for i, r := range formatsRaw {
			resFormats[i] = r.Data().(string)
		}
	} else {
		resFormats = make([]string, 0)
	}

	dr := &model.DataResource{
		ResID:        resID,
		ResPath:      resPath,
		ResType:      model.ResTypeTime,
		ResFormat:    resFormats,
		IsCollection: true,
		Variables:    make([]*model.Variable, 0),
	}

	for _, v := range schemaVariables {
		variable, err := parseSchemaVariable(v, dr.Variables, false)
		if err != nil {
			return nil, err
		}
		dr.Variables = append(dr.Variables, variable)
	}

	return dr, nil
}
