package metadata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
)

func (m *Metadata) loadOriginalSchemaResourceTable(res *gabs.Container) (*DataResource, error) {
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
		variable, err := m.parseSchemaVariable(v)
		if err != nil {
			return nil, err
		}
		dr.Variables = append(dr.Variables, variable)
	}

	return dr, nil
}
