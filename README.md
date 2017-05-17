# distil-ingest

## Dependencies

Requires the [Go](https://golang.org/) programming language binaries with the `GOPATH` environment variable specified and `$GOPATH/bin` in your `PATH`.

## Installation

```bash
go get github.com/unchartedsoftware/distil-ingest
```

## Development

Clone the repository:

```bash
mkdir $GOPATH/src/github.com/unchartedsoftware
cd $GOPATH/src/github.com/unchartedsoftware
git clone git@github.com:unchartedsoftware/distil-ingest.git
```

Install dependencies:

```bash
cd distil-ingest
make install
```

## Usage

This application provides [deluge](https://github.com/unchartedsoftware/deluge) document implementations for bulk ingests of D3M track 2 datasets into [elasticsearch](https://github.com/elastic/elasticsearch).

To ingest:
1.  Download D3M datasets of interest from <https://datadrivendiscovery.org/data> and unzip.
2.  Run the ingest for each dataset: 

```bash
./d3m-ingest -es-endpoint "http://some-es-instance.com:9200" -es-index "o_28" -dataset-path "/data/d3m/o_28"
```

## Common Issues:

#### "EOF"

- The Elasticsearch instance does not have `http.compression` enabled.
- The `mappings` json argument is invalid, most likely missing a closing bracket

#### "No Elasticsearch node available"

- You are accessing an Elasticsearch instance that requires a VPN and it is not on.
- The Elasticsearch instance is temporarily down.
