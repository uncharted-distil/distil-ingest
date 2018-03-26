package metadata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
)

// Table is a data respurce that is contained within one or many tabular files.
type Table struct {
}

// Parse extracts the data resource from the data schema document.
func (r *Table) Parse(res *gabs.Container) (*DataResource, error) {
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

	dr := &DataResource{
		ResID:        resID,
		ResPath:      resPath,
		ResType:      resTypeTable,
		IsCollection: false,
		Variables:    make([]*Variable, 0),
	}

	for _, v := range schemaVariables {
		variable, err := parseSchemaVariable(v)
		if err != nil {
			return nil, err
		}
		dr.Variables = append(dr.Variables, variable)
	}

	return dr, nil
}
