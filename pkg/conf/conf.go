//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package conf

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
	DatasetFolder   string

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
	DBHost      string
	DBPort      int

	// control flags
	IncludeRaw   bool
	MetadataOnly bool
}
