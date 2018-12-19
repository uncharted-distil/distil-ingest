package metadata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/distil-compute/model"
)

// Timeseries is a data resource that is contained within one or many timeseries files.
type Timeseries struct {
}

// Parse extracts the data resource from the data schema document.
func (r *Timeseries) Parse(res *gabs.Container) (*model.DataResource, error) {
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
