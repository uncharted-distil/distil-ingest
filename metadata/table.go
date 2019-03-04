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

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
)

// Table is a data respurce that is contained within one or many tabular files.
type Table struct {
}

// Parse extracts the data resource from the data schema document.
func (r *Table) Parse(res *gabs.Container) (*model.DataResource, error) {
	schemaVariables, err := res.Path("columns").Children()
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse column data")
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
		formatsRaw, err := res.Path("resFormat").Children()
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse resource format")
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
