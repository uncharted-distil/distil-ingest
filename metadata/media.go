package metadata

import (
	"fmt"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
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
func (r *Media) Parse(res *gabs.Container) (*DataResource, error) {
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

	dr := &DataResource{
		ResID:        resID,
		ResPath:      resPath,
		ResType:      r.Type,
		IsCollection: true,
		ResFormat:    resFormats,
	}

	return dr, nil
}
