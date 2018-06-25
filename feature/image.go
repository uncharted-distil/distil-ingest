package feature

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/rest"
)

type potentialFeature struct {
	originalResPath string
	newVariable     *metadata.Variable
}

func getDataResource(meta *metadata.Metadata, resID string) *metadata.DataResource {
	// main data resource has d3m index variable
	for _, dr := range meta.DataResources {
		if dr.ResID == resID {
			return dr
		}
	}

	return nil
}

// FeaturizeDataset reads adds features based on referenced data resources
// in the metadata. The features are added as a reference resource in
// the metadata and written to the output path.
func FeaturizeDataset(meta *metadata.Metadata, imageFeaturizer *rest.Featurizer, sourcePath string, mediaPath string, outputFolder string, outputPathData string, outputPathSchema string, hasHeader bool) error {
	// find the main data resource
	mainDR := meta.GetMainDataResource()

	// featurize image columns
	log.Infof("adding features to schema")
	colsToFeaturize := addFeaturesToSchema(meta, mainDR)

	// read the data to process every row
	log.Infof("opening data from source")
	dataPath := path.Join(sourcePath, mainDR.ResPath)
	csvFile, err := os.Open(dataPath)
	if err != nil {
		return errors.Wrap(err, "failed to open data file")
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// write the header as needed
	if hasHeader {
		header := make([]string, len(mainDR.Variables))
		for _, v := range mainDR.Variables {
			header[v.Index] = v.Name
		}
		err = writer.Write(header)
		if err != nil {
			return errors.Wrap(err, "error writing header to output")
		}
	}

	// skip header
	if hasHeader {
		_, err = reader.Read()
		if err != nil {
			return errors.Wrap(err, "failed to read header from file")
		}
	}

	log.Infof("reading data from source")
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "failed to read line from file")
		}
		// featurize the row as necessary
		for index, colDR := range colsToFeaturize {
			imagePath := fmt.Sprintf("%s/%s", mediaPath, path.Join(colDR.originalResPath, line[index]))
			log.Infof("Featurizing %s", imagePath)
			feature, err := featurizeImage(imagePath, imageFeaturizer)
			if err != nil {
				return errors.Wrap(err, "error getting image feature output")
			}

			// add the feature output
			line = append(line, feature)
		}

		writer.Write(line)
		if err != nil {
			return errors.Wrap(err, "error storing featured output")
		}
	}

	// output the data
	log.Infof("Writing data to output")
	dataPathToWrite := path.Join(outputFolder, outputPathData)
	writer.Flush()
	err = ioutil.WriteFile(dataPathToWrite, output.Bytes(), 0644)
	if err != nil {
		return errors.Wrap(err, "error writing feature output")
	}

	// main DR should point to new file
	mainDR.ResPath = outputPathData

	// output the schema
	log.Infof("Writing schema to output")
	schemaPathToWrite := path.Join(outputFolder, outputPathSchema)
	err = meta.WriteSchema(schemaPathToWrite)

	return err
}

func addFeaturesToSchema(meta *metadata.Metadata, mainDR *metadata.DataResource) map[int]*potentialFeature {
	colsToFeaturize := make(map[int]*potentialFeature)
	for _, v := range mainDR.Variables {
		if v.RefersTo != nil && v.RefersTo["resID"] != nil {
			// get the refered DR
			resID := v.RefersTo["resID"].(string)

			res := getDataResource(meta, resID)

			// check if needs to be featurized
			if res.CanBeFeaturized() {
				// create the new resource to hold the featured output
				indexName := fmt.Sprintf("_feature_%s", v.Name)

				// add the feature variable
				refVariable := &metadata.Variable{
					Name:             indexName,
					Index:            len(mainDR.Variables),
					Type:             "string",
					Role:             []string{"attribute"},
					DistilRole:       metadata.VarRoleMetadata,
					OriginalVariable: v.Name,
				}
				mainDR.Variables = append(mainDR.Variables, refVariable)

				colsToFeaturize[v.Index] = &potentialFeature{
					originalResPath: res.ResPath,
					newVariable:     refVariable,
				}
			}
		}
	}

	return colsToFeaturize
}

func featurizeImage(filepath string, featurizer *rest.Featurizer) (string, error) {
	feature, err := featurizer.FeaturizeImage(filepath)
	if err != nil {
		return "", errors.Wrap(err, "failed to featurize image")
	}

	objs, ok := feature.Image["objects"].(map[string]interface{})
	if !ok {
		return "", errors.Wrap(err, "image feature objects in unexpected format")
	}

	labels, ok := objs["label"].(map[string]interface{})
	if !ok {
		return "", errors.Wrap(err, "image feature labels in unexpected format")
	}

	labelText := make([]string, 0)
	for _, l := range labels {
		cleanedLabel := strings.Replace(l.(string), "_", " ", -1)
		labelText = append(labelText, cleanedLabel)
	}

	return strings.Join(labelText, ","), nil
}
