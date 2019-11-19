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
	"path"

	"github.com/Jeffail/gabs/v2"
	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/model"
)

// Raw is a data resource that is contained within one file which does not
// have fields specified in the schema.
type Raw struct {
	rootPath string
}

// Parse extracts the data resource from the data schema document.
func (r *Raw) Parse(res *gabs.Container) (*model.DataResource, error) {
	if res.Path("resID").Data() == nil {
		return nil, errors.Errorf("unable to parse resource id")
	}
	resID := res.Path("resID").Data().(string)

	if res.Path("resPath").Data() == nil {
		return nil, errors.Errorf("unable to parse resource path")
	}
	resPath := res.Path("resPath").Data().(string)

	resFormats, err := parseResFormats(res)
	if err != nil {
		return nil, err
	}

	dr, err := loadRawVariables(path.Join(r.rootPath, resPath))
	if err != nil {
		return nil, err
	}

	dr.ResPath = resPath
	dr.ResID = resID
	dr.ResType = model.ResTypeRaw
	dr.ResFormat = resFormats

	return dr, nil
}
