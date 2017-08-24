package conf

var config *Conf

// Conf represents all the ingest runtime flags passed to the binary.
type Conf struct {
	// elasticsearch config
	ESEndpoint     string
	ESIndex        string
	DocType        string
	ClearExisting  bool
	BulkByteSize   int64
	ScanBufferSize int

	// d3m dataset directory path
	SchemaPath  string
	DatasetPath string

	// num workers
	NumWorkers int
	// num es connections
	NumActiveConnections int
	// error threshold
	ErrThreshold float64

	// postgres config
	Database   string
	DBTable    string
	DBUser     string
	DBPassword string
}
