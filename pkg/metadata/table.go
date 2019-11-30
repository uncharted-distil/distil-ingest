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

// Table is a data respurce that is contained within one or many tabular files.
type Table struct {
}

// Parse extracts the data resource from the data schema document.
func (r *Table) Parse(res *gabs.Container) (*model.DataResource, error) {
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

	resFormats, err := parseResFormats(res)
	if err != nil {
		return nil, err
	}

	dr := &model.DataResource{
		ResID:        resID,
		ResPath:      resPath,
		ResType:      model.ResTypeTable,
		ResFormat:    resFormats,
		IsCollection: false,
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

func parseResFormats(res *gabs.Container) (map[string][]string, error) {
	var resFormats map[string][]string
	if res.Path("resFormat").Data() != nil {
		formatsRaw := res.Path("resFormat").ChildrenMap()
		if formatsRaw == nil {
			return nil, errors.New("unable to parse resource format")
		}
		resFormats = make(map[string][]string)
		for typ, r := range formatsRaw {
			formatsNestedRaw := r.Children()
			if formatsNestedRaw == nil {
				return nil, errors.New("unable to parse nested resource format")
			}
			resFormats[typ] = make([]string, len(formatsNestedRaw))
			for j, rn := range formatsNestedRaw {
				resFormats[typ][j] = rn.Data().(string)
			}
		}
	} else {
		resFormats = make(map[string][]string, 0)
	}

	return resFormats, nil
}
