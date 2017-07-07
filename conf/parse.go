package conf

import (
	"errors"
	"flag"
)

// ParseCommandLine parses the commandline arguments and returns a Conf object.
func ParseCommandLine() (*Conf, error) {
	// es output
	esEndpoint := flag.String("es-endpoint", "", "Elasticsearch endpoint")
	esIndex := flag.String("es-index", "", "Elasticsearch index")
	bulkByteSize := flag.Int("batch-size", 1024*1024*20, "The bulk batch size in bytes")
	scanBufferSize := flag.Int("scan-size", 1024*1024*2, "The size of the buffer allocated for each input row")
	clearExisting := flag.Bool("clear-existing", true, "Clear index before ingest")

	// filesystem
	datasetPath := flag.String("dataset-path", "", "Filesystem input path")

	// num workers
	numWorkers := flag.Int("num-workers", 8, "The worker pool size")
	// num es connections
	numActiveConnections := flag.Int("num-active-connections", 8, "The number of concurrent outgoing connections")
	// error threshold
	errThreshold := flag.Float64("error-threshold", 0.01, "The percentage threshold of unsuccessful documents which when passed will end ingestion")

	// postgres
	database := flag.String("database", "", "Postgres database")
	dbTable := flag.String("db-table", "", "Postgres database table")
	dbUser := flag.String("db-user", "", "Postgres database user")
	dbPassword := flag.String("db-password", "", "Postgres database password")

	// parse the flags
	flag.Parse()

	// check required flags
	if *esEndpoint == "" && *database == "" {
		return nil, errors.New("ElasticSearch endpoint and Database are not specified, please provide CL arg '-es-endpoint' or '-database'")
	}
	if *esIndex == "" && *dbTable == "" {
		return nil, errors.New("ElasticSearch index and DB Table are not specified, please provide CL arg '-es-index' or 'db-table'")
	}

	// Set and save config
	return &Conf{
		ESEndpoint:     *esEndpoint,
		ESIndex:        *esIndex,
		ClearExisting:  *clearExisting,
		BulkByteSize:   int64(*bulkByteSize),
		ScanBufferSize: *scanBufferSize,
		// file
		DatasetPath: *datasetPath,

		// num of workers
		NumWorkers: *numWorkers,
		// num es connections
		NumActiveConnections: *numActiveConnections,
		// error threshold
		ErrThreshold: *errThreshold,

		// postgres
		Database:   *database,
		DBTable:    *dbTable,
		DBUser:     *dbUser,
		DBPassword: *dbPassword,
	}, nil
}
