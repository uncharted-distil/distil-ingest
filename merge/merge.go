package merge

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
)

func parseD3MIndex(schema *gabs.Container, path string) (int, error) {
	// find the row ID column and store it for quick retrieval
	trainingArray, err := schema.Path(path).Children()
	if err != nil {
		return -1, err
	}
	for index, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})
		if varDesc["varName"].(string) == "d3mIndex" {
			return index, nil
		}
	}
	return -1, fmt.Errorf("d3mIndex column not found for path %s", path)
}

// JoinIndices provides the column indices to join the left and right csvs on
type JoinIndices struct {
	LeftColIdx  int
	RightColIdx int
}

// GetColIndices will get the indices of the 'd3mIndex' column for the training and training target
// files from a dataset schema
func GetColIndices(schemaPath string, columnName string) (*JoinIndices, error) {
	// Open the schema file
	dat, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file")
	}

	// Unmarshall the schema file
	schema, err := gabs.ParseJSON(dat)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshall %s", schemaPath)
	}

	// Extract d3mIndex cols
	trainIndex, err := parseD3MIndex(schema, "trainData.trainData")
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse train data")
	}

	targetsIndex, err := parseD3MIndex(schema, "trainData.trainTargets")
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse train targets")
	}

	return &JoinIndices{LeftColIdx: trainIndex, RightColIdx: targetsIndex}, nil
}

func buildIndex(filename string, colIdx int, hasHeader bool, includeHeader bool) (map[string][]string, error) {
	// load data from csv file into a map indexed by the values from the specified column
	var index = make(map[string][]string)
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", filename)
	}
	reader := csv.NewReader(file)
	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrapf(err, "failed read line %d from %s", count, filename)
		}

		if count > 0 || !hasHeader || (hasHeader && includeHeader) {
			key := line[colIdx]
			index[key] = append(line[:colIdx], line[colIdx+1:]...)
		}
		count++
	}
	file.Close()
	return index, nil
}

// LeftJoin provides a function to join to csv files based on the specified column
func LeftJoin(leftFile string, leftCol int, rightFile string, rightCol int, hasHeader bool, includeHeader bool) ([]byte, int, int, error) {

	// load the right file into a hash table indexed by the d3mIndex col
	index, err := buildIndex(rightFile, rightCol, hasHeader, includeHeader)
	if err != nil {
		return nil, -1, -1, err
	}

	// open the left and outfiles for line-by-line by processing
	leftCsvFile, err := os.Open(leftFile)
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "failed to open left operand file")
	}

	// perform a left join, leaving unmatched right values emptys
	reader := csv.NewReader(leftCsvFile)

	// output writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	var count = 0
	var missed = 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, -1, -1, errors.Wrap(err, "failed to read line from left operand file")
		}
		if count > 0 || !hasHeader || (hasHeader && includeHeader) {
			key := line[leftCol]
			rightVals, ok := index[key]
			if !ok {
				rightVals = []string{}
				missed++
			}
			line = append(line, rightVals...)
			// write the csv line back out
			writer.Write(line)
		}

		count++
	}
	// flush writer
	writer.Flush()

	// close left
	err = leftCsvFile.Close()
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "failed to close left input file")
	}

	return output.Bytes(), count, missed, nil
}
