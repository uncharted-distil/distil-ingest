package conf

var config *Conf

// Conf represents all the ingest runtime flags passed to the binary.
type Conf struct {
	// elasticsearch config
	ESEndpoint      string
	ESIndex         string
	ESDatasetPrefix string
	DocType         string
	ClearExisting   bool
	BulkByteSize    int64
	ScanBufferSize  int

	// d3m dataset directory path
	TypeSource         string
	ClassificationPath string
	ImportancePath     string
	SummaryPath        string
	SummaryMachinePath string
	SchemaPath         string
	DatasetPath        string

	// num workers
	NumWorkers int
	// num es connections
	NumActiveConnections int
	// thresholds
	ErrThreshold         float64
	ProbabilityThreshold float64

	// postgres config
	Database    string
	DBTable     string
	DBUser      string
	DBPassword  string
	DBBatchSize int

	// control flags
	IncludeRaw   bool
	MetadataOnly bool
}
