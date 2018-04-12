package metadata

import (
	"github.com/jeffail/gabs"
)

// DataResourceParser is a parser for a data resource in the schema document.
type DataResourceParser interface {
	Parse(res *gabs.Container) (*DataResource, error)
}
