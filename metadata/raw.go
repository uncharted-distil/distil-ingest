package metadata

import (
	"path"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/distil-compute/model"
)

// Raw is a data resource that is contained within one file which does not
// have fields specified in the schema.
type Raw struct {
	rootPath string
}

// Parse extracts the data resource from the data schema document.
func (r *Raw) Parse(res *gabs.Container) (*model.DataResource, error) {
	if res.Path("resPath").Data() == nil {
		return nil, errors.Errorf("unable to parse resource path")
	}
	resPath := res.Path("resPath").Data().(string)

	dr, err := loadRawVariables(path.Join(r.rootPath, resPath))
	if err != nil {
		return nil, err
	}

	return dr, nil
}
