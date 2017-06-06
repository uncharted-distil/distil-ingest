package merge

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
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
		return nil, err
	}

	// Unmarshall the schema file
	schema, err := gabs.ParseJSON(dat)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to unmarshall %s", schemaPath)
	}

	// Extract d3mIndex cols
	trainIndex, err := parseD3MIndex(schema, "trainData.trainData")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse train data")
	}

	targetsIndex, err := parseD3MIndex(schema, "trainData.trainTargets")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse train targets")
	}

	return &JoinIndices{LeftColIdx: trainIndex, RightColIdx: targetsIndex}, nil
}

func buildIndex(file string, colIdx int, header bool) (map[string][]string, error) {
	// load data from csv file into a map indexed by the values from the specified column
	var index = make(map[string][]string)

	csvFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(bufio.NewReader(csvFile))

	var lineCnt = 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if lineCnt > 0 || !header {
			key := line[colIdx]
			index[key] = append(line[:colIdx], line[colIdx+1:]...)
		}
		lineCnt++
	}
	csvFile.Close()

	return index, nil
}

// LeftJoin provides a function to join to csv files based on the specified column
func LeftJoin(leftFile string, leftCol int, rightFile string, rightCol int, outFile string, header bool) error {

	log.Infof("Joining %s, %s on indices %d, %d", leftFile, rightFile, leftCol, rightCol)

	// load the right file into a hash table indexed by the d3mIndex col
	index, err := buildIndex(rightFile, rightCol, header)
	if err != nil {
		return errors.Wrap(err, "Failed to be build right operand LUT")
	}

	// open the left and outfiles for line-by-line by processing
	leftCsvFile, err := os.Open(leftFile)
	if err != nil {
		return errors.Wrap(err, "Failed to open left operand file")
	}
	outCsvFile, err := os.Create(outFile)
	if err != nil {
		return errors.Wrap(err, "Failed to open join result output file")
	}

	// perform a left join, leaving unmatched right values emptys
	reader := csv.NewReader(bufio.NewReader(leftCsvFile))
	writer := csv.NewWriter(bufio.NewWriter(outCsvFile))
	var lineCnt = 0
	var missedCnt = 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "Failed to read line from left operand file")
		}
		if lineCnt > 0 || !header {
			key := line[leftCol]
			rightVals, ok := index[key]
			if !ok {
				rightVals = []string{}
				missedCnt++
			}
			line = append(line, rightVals...)
			// write the csv line back out
			writer.Write(line)
		}

		lineCnt++
	}
	writer.Flush()

	leftCsvFile.Close()
	outCsvFile.Close()

	log.Infof("Merged %d lines, %d lines unmatched", lineCnt, missedCnt)

	return nil
}
