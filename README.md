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

Build executable:

```bash
make build
```

## Usage

The repository contains CLIs used to parse, and ingest 3M OpenML datasets (those with a name beginning with `o_`) into [elasticsearch](https://github.com/elastic/elasticsearch).

#### Merging training and target datasets:

- Download D3M datasets of interest from <https://datadrivendiscovery.org/data> and unzip.
- Update and ensure the arguments in `./merge_all.sh`are correct
- Run `./merge_all.sh`

#### Classifying merged datasets:

- Update and ensure the arguments in `./classify_all.sh`are correct
- Run `./classify_all.sh`

#### Ingesting merged and classified datasets:

- Update and ensure the arguments in `./ingest_all.sh`are correct
- Run `./ingest_all.sh`

## Common Issues:

#### "EOF"

- The Elasticsearch instance does not have `http.compression` enabled.
- The `mappings` json argument is invalid, most likely missing a closing bracket

#### "No Elasticsearch node available"

- You are accessing an Elasticsearch instance that requires a VPN and it is not on.
- The Elasticsearch instance is temporarily down.
