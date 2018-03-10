package metadata

import (
	"fmt"

	"github.com/jeffail/gabs"
)

func (m *Metadata) loadOriginalSchemaResourceImage(res *gabs.Container) (*DataResource, error) {
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
		ResType:      resTypeImage,
		IsCollection: true,
	}

	return dr, nil
}
