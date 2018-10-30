package compute

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/util"
)

const (
	// D3MDataSchema provides the name of the D3M data schema file
	D3MDataSchema = "datasetDoc.json"

	trainFilenamePrefix = "train"
	testFilenamePrefix  = "test"
)

func updateSchemaReferenceFile(schema string, prevReferenceFile string, newReferenceFile string) string {
	return strings.Replace(schema, fmt.Sprintf("\"resPath\": \"%s\"", prevReferenceFile), fmt.Sprintf("\"resPath\": \"%s\"", newReferenceFile), 1)
}

func splitTrainTest(sourceFile string, trainFile string, testFile string, hasHeader bool) error {
	// create the writers
	outputTrain := &bytes.Buffer{}
	writerTrain := csv.NewWriter(outputTrain)
	outputTest := &bytes.Buffer{}
	writerTest := csv.NewWriter(outputTest)

	// open the file
	file, err := os.Open(sourceFile)
	if err != nil {
		return errors.Wrap(err, "failed to open source file")
	}
	reader := csv.NewReader(file)

	// write header to both outputs
	if hasHeader {
		header, err := reader.Read()
		if err != nil {
			return errors.Wrap(err, "unable to read header row")
		}
		err = writerTrain.Write(header)
		if err != nil {
			return errors.Wrap(err, "unable to write header to train output")
		}
		err = writerTest.Write(header)
		if err != nil {
			return errors.Wrap(err, "unable to write header to test output")
		}
	}

	// randomly assign rows to either train or test
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "failed to read line from file")
		}
		if rand.Float64() < trainTestSplitThreshold {
			err = writerTrain.Write(line)
			if err != nil {
				return errors.Wrap(err, "unable to write data to train output")
			}
		} else {
			err = writerTest.Write(line)
			if err != nil {
				return errors.Wrap(err, "unable to write data to test output")
			}
		}
	}
	writerTrain.Flush()
	writerTest.Flush()

	err = util.WriteFileWithDirs(trainFile, outputTrain.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to output train data")
	}

	err = util.WriteFileWithDirs(testFile, outputTest.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to output test data")
	}

	return nil
}

// PersistOriginalData copies the original data and splits it into a train &
// test subset to be used as needed.
func PersistOriginalData(datasetName string, schemaFile string, sourceDataFolder string, tmpDataFolder string) (string, string, error) {
	// The complete data is copied into separate train & test folders.
	// The main data is then split randomly.
	trainFolder := path.Join(tmpDataFolder, datasetName, trainFilenamePrefix)
	testFolder := path.Join(tmpDataFolder, datasetName, testFilenamePrefix)
	trainSchemaFile := path.Join(trainFolder, schemaFile)
	testSchemaFile := path.Join(testFolder, schemaFile)

	// check if the data has already been split
	log.Infof("checking folders `%s` & `%s` to see if the dataset has been previously split", trainFolder, testFolder)
	if util.FileExists(trainSchemaFile) && util.FileExists(testSchemaFile) {
		log.Infof("dataset '%s' already split", datasetName)
		return trainSchemaFile, testSchemaFile, nil
	}

	if util.DirExists(trainFolder) {
		err := os.RemoveAll(trainFolder)
		if err != nil {
			return "", "", errors.Wrap(err, "unable to remove train folder from previous split attempt")
		}
	}

	if util.DirExists(testFolder) {
		err := os.RemoveAll(testFolder)
		if err != nil {
			return "", "", errors.Wrap(err, "unable to remove test folder from previous split attempt")
		}
	}

	// copy the data over
	err := copy.Copy(sourceDataFolder, trainFolder)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to copy dataset folder to train")
	}

	err = copy.Copy(sourceDataFolder, testFolder)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to copy dataset folder to test")
	}

	// read the dataset document
	schemaFilename := path.Join(sourceDataFolder, schemaFile)
	meta, err := metadata.LoadMetadataFromOriginalSchema(schemaFilename)
	if err != nil {
		return "", "", err
	}

	// determine where the d3m index would be
	mainDR := meta.GetMainDataResource()

	// split the source data into train & test
	dataPath := path.Join(sourceDataFolder, mainDR.ResPath)
	trainDataFile := path.Join(trainFolder, mainDR.ResPath)
	testDataFile := path.Join(testFolder, mainDR.ResPath)
	err = splitTrainTest(dataPath, trainDataFile, testDataFile, true)
	if err != nil {
		return "", "", err
	}

	return trainSchemaFile, testSchemaFile, nil
}
