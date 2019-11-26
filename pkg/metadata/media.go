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

	"github.com/uncharted-distil/distil-compute/model"
)

// Media is a data resource that is backed by media files.
type Media struct {
	Type string
}

// NewMedia creates a new Media instance.
func NewMedia(typ string) *Media {
	return &Media{
		Type: typ,
	}
}

// Parse extracts the data resource from the data schema document.
func (r *Media) Parse(res *gabs.Container) (*model.DataResource, error) {
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
		ResType:      r.Type,
		IsCollection: true,
		ResFormat:    resFormats,
	}

	return dr, nil
}
