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

	"github.com/unchartedsoftware/distil-ingest/metadata"
)

const (
	d3mIndexName = "d3mIndex"
)

// FileLink represents a link between a dataset col and a file.
type FileLink struct {
	Name      string
	IndexVar  *metadata.Variable
	Lookup    map[string][]string
	Header    []string
	Variables []*metadata.Variable
}

func readFileLink(dataResource *metadata.DataResource, filename string) (*FileLink, error) {
	// open file
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", filename)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// read header
	header, err := reader.Read()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read header from csv file %s", filename)
	}

	// read rows
	var rows [][]string
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrapf(err, "failed read from %s", filename)
		}
		rows = append(rows, line)
	}

	// create map of indices in the dataset
	var indexVar *metadata.Variable
	variables := make([]*metadata.Variable, 0)
	for _, variable := range dataResource.Variables {
		if variable.SelectedRole == "index" {
			indexVar = variable
		} else {
			variables = append(variables, variable)
		}
	}

	// build lookups for each row
	lookup := make(map[string][]string)
	for _, row := range rows {
		// copy row, without the index
		var rowWithoutIndex []string
		rowWithoutIndex = append(rowWithoutIndex, row[0:indexVar.Index]...)
		rowWithoutIndex = append(rowWithoutIndex, row[indexVar.Index+1:]...)
		indexVal := row[indexVar.Index]
		lookup[indexVal] = rowWithoutIndex
	}

	// copy header, without the index
	var headerWithoutIndex []string
	headerWithoutIndex = append(headerWithoutIndex, header[0:indexVar.Index]...)
	headerWithoutIndex = append(headerWithoutIndex, header[indexVar.Index+1:]...)

	return &FileLink{
		Name:      filename,
		IndexVar:  indexVar,
		Lookup:    lookup,
		Header:    headerWithoutIndex,
		Variables: variables,
	}, nil
}

// InjectFileLinksFromFile traverses all file links and injests the relevant data.
func InjectFileLinksFromFile(meta *metadata.Metadata, inputFilename string, rawDataPath string, hasHeader bool) (*metadata.DataResource, []byte, error) {
	// need to skip the header row.
	var data []byte
	var err error
	if hasHeader {
		csvFile, err := os.Open(inputFilename)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to open file")
		}
		defer csvFile.Close()

		reader := csv.NewReader(csvFile)
		output := &bytes.Buffer{}
		writer := csv.NewWriter(output)

		var count = 0
		for {
			line, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, nil, errors.Wrap(err, "failed to read line from file")
			}

			if count > 0 || !hasHeader {
				writer.Write(line)
			}

			count++
		}
		writer.Flush()
		data = output.Bytes()
	} else {
		data, err = ioutil.ReadFile(inputFilename)

		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to read input file for injection")
		}
	}

	return InjectFileLinks(meta, data, rawDataPath)
}

// InjectFileLinks traverses all file links and injests the relevant data.
func InjectFileLinks(meta *metadata.Metadata, merged []byte, rawDataPath string) (*metadata.DataResource, []byte, error) {
	// determine if there are any links
	// assume the main data resource is the one with a key column
	mergedDataResource := &metadata.DataResource{}
	dataResources := make(map[string]*metadata.DataResource)
	indexColumns := make(map[string]*metadata.Variable)
	keyColumns := make([]*metadata.Variable, 0)
	for _, dr := range meta.DataResources {
		dataResources[dr.ResID] = dr
		for i := 0; i < len(dr.Variables); i++ {
			variable := dr.Variables[i]
			if variable.SelectedRole == "index" {
				indexColumns[variable.Name] = variable
			} else if variable.SelectedRole == "key" && variable.RefersTo != nil {
				keyColumns = append(keyColumns, variable)
			} else if variable.Name == d3mIndexName {
				mergedDataResource.Variables = dr.Variables
			}
		}
	}

	// for every key column, load the relevant file
	links := make(map[string]*FileLink)
	if len(keyColumns) > 0 {
		for _, variable := range keyColumns {
			if variable.RefersTo.Path("resID").Data() == nil {
				continue
			}
			resID := variable.RefersTo.Path("resID").Data().(string)

			res := dataResources[resID]
			l, err := readFileLink(res, fmt.Sprintf("%s/%s", rawDataPath, res.ResPath))
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed read file link %s from resource %s", res.ResPath, res.ResPath)
			}
			links[variable.Name] = l

			mergedDataResource.Variables = append(mergedDataResource.Variables, l.Variables...)
		}
	}

	// create reader
	reader := csv.NewReader(bytes.NewBuffer(merged))

	// output writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, errors.Wrapf(err, "failed read to line %d", count)
		}

		// NOTE: there is no header by this step

		// process each link
		for i := 0; i < len(keyColumns); i++ {
			key := keyColumns[i]
			link := links[key.Name]
			// find link index in line
			linkIndex := line[key.Index]
			// look up row in link file based on link index
			linkedRow := link.Lookup[linkIndex]

			// inject row into line
			line = append(line, linkedRow...)
		}

		// write the output
		writer.Write(line)
		count++
	}

	writer.Flush()

	return mergedDataResource, output.Bytes(), nil
}

func parseD3MIndex(schema *gabs.Container, path string) (int, error) {
	// find the row ID column and store it for quick retrieval
	trainingArray, err := schema.Path(path).Children()
	if err != nil {
		return -1, err
	}
	for index, value := range trainingArray {
		varDesc := value.Data().(map[string]interface{})
		if varDesc["varName"].(string) == d3mIndexName {
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

func buildIndex(filename string, colIdx int, hasHeader bool) (map[string][]string, error) {
	// load data from csv file into a map indexed by the values from the specified column
	var index = make(map[string][]string)
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", filename)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrapf(err, "failed read line %d from %s", count, filename)
		}

		if count > 0 || !hasHeader {
			key := line[colIdx]
			index[key] = append(line[:colIdx], line[colIdx+1:]...)
		}
		count++
	}
	return index, nil
}

// LeftJoin provides a function to join to csv files based on the specified column
func LeftJoin(leftFile string, leftCol int, rightFile string, rightCol int, hasHeader bool) ([]byte, int, int, error) {

	// load the right file into a hash table indexed by the d3mIndex col
	index, err := buildIndex(rightFile, rightCol, hasHeader)
	if err != nil {
		return nil, -1, -1, err
	}

	// open the left and outfiles for line-by-line by processing
	leftCsvFile, err := os.Open(leftFile)
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "failed to open left operand file")
	}
	defer leftCsvFile.Close()

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
		if count > 0 || !hasHeader {
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

	return output.Bytes(), count, missed, nil
}
