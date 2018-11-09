package metadata

import (
	"github.com/jeffail/gabs"

	"github.com/unchartedsoftware/distil-compute/model"
)

// DataResourceParser is a parser for a data resource in the schema document.
type DataResourceParser interface {
	Parse(res *gabs.Container) (*model.DataResource, error)
}
