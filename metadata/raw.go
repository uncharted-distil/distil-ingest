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
	if res.Path("resID").Data() == nil {
		return nil, errors.Errorf("unable to parse resource id")
	}
	resID := res.Path("resID").Data().(string)

	if res.Path("resPath").Data() == nil {
		return nil, errors.Errorf("unable to parse resource path")
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
