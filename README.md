# veldt-ingest

## Dependencies

Requires the [Go](https://golang.org/) programming language binaries with the `GOPATH` environment variable specified and `$GOPATH/bin` in your `PATH`.

## Installation

```bash
go get github.com/unchartedsoftware/veldt-ingest
```

## Development

Clone the repository:

```bash
mkdir $GOPATH/src/github.com/unchartedsoftware
cd $GOPATH/src/github.com/unchartedsoftware
git clone git@github.com:unchartedsoftware/veldt-ingest.git
```

Install dependencies:

```bash
cd veldt-ingest
make install
```

## Usage

This application provides [deluge](https://github.com/unchartedsoftware/deluge) document implementations for customizable bulk ingests of data into [elasticsearch](https://github.com/elastic/elasticsearch).

### 1) Create the Document Implementation

Create the `document.go` file in the appropriate package:

```bash
mkdir -p document/sample
touch document/sample/document.go
```

Implement the `deluge.Document` interface:

```go
package sample

import (
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"
)

// Document overrides the CSV document type.
type Document struct {
	document.CSV
}

type source {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Timestamp int64 `json:"timestamp"`
}

// NewDocument instantiates and returns a new document.
func NewDocument() (deluge.Document, error) {
	return &Document{}, nil
}

// GetID returns the document's id.
func (d *Document) GetID() (string, error) {
	id, ok := d.GetString(0) // first col is the id
	if !ok {
		return fmt.Errorf("unable to parse `id` from document source")
	}
	return id, nil
}

// GetType returns the document's type.
func (d *Document) GetType() (string, error) {
	return "datum", nil
}

// GetMapping returns the document's mapping.
func (d *Document) GetMapping() (string, error) {
	return `{
		"datum": {
			"properties":{
				"name": {
					"type": "string"
				},
				"timestamp": {
					"type": "date",
					"format": "strict_date_optional_time||epoch_millis"
				},
				"size": {
					"type": "long"
				}
			}
		}
	}`, nil
}

// GetSource returns the source portion of the document.
func (d *Document) GetSource() (interface{}, error) {
	name, ok := d.GetString(1)
	if !ok {
		return nil, fmt.Errorf("unable to parse `name` from document source")
	}
	size, ok := d.GetInt64(2)
	if !ok {
		return nil, fmt.Errorf("unable to parse `size` from document source")
	}
	timestamp, ok := d.GetInt64(3)
	if !ok {
		return nil, fmt.Errorf("unable to parse `timestamp` from document source")
	}
	return &source {
		Name: name,
		Size: size,
		Timestamp timestamp,
	}
}
```

Be sure to run `make build` to ensure the package compiles and meets the `golint` and `go vet` standards. Then run `make fmt` to format the code.

### 2) Register Document in `main.go`

Import the package:

```go
import (
	"github.com/unchartedsoftware/veldt-ingest/document/sample"
)
```

Register the document:

```go
document.Register("sample", sample.NewDocument)
```

### 3) Add CLI entry to `ingest.sh`

Add a clause to the case statement:

```bash

	sample)

		echo
		echo Ingesting Sample Data
		FILE_SAMPLE_PATH="/sample/path/"
		ES_SAMPLE_INDEX="sample_index_v0"

		go run main.go \
			-es-endpoint="http://10.64.16.120:9200" \
			-es-index="sample_index_v0" \
			-doc-type="sample" \
			-input-type="file" \
			-file-input-path="/sample/data/path"
		;;

```

### 4) Run the Ingest Script

Execute the ingest script passing the appropriate clause identifier.

```bash
./ingest.sh sample
```

## Common Issues:

#### "EOF"

- The Elasticsearch instance does not have `http.compression` enabled.
- The `mappings` json argument is invalid, most likely missing a closing bracket

#### "No Elasticsearch node available"

- You are accessing an Elasticsearch instance that requires a VPN and it is not on.
- The Elasticsearch instance is temporarily down.
