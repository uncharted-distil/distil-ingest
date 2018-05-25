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
	newDataResource *metadata.DataResource
}

func getMainDataResource(meta *metadata.Metadata) *metadata.DataResource {
	// main data resource has d3m index variable
	for _, dr := range meta.DataResources {
		for _, v := range dr.Variables {
			if v.Name == metadata.D3MIndexName {
				return dr
			}
		}
	}

	return nil
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
func FeaturizeDataset(meta *metadata.Metadata, imageFeaturizer *rest.Featurizer, sourcePath string, mediaPath string, outputPath string, hasHeader bool) error {
	// find the main data resource
	mainDR := getMainDataResource(meta)

	// featurize image columns
	log.Infof("adding features to schema")
	d3mIndexFieldIndex, colsToFeaturize := addFeaturesToSchema(meta, mainDR)

	// read the data to process every row
	log.Infof("opening data from source")
	dataPath := path.Join(sourcePath, mainDR.ResPath)
	csvFile, err := os.Open(dataPath)
	if err != nil {
		return errors.Wrap(err, "failed to open data file")
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)

	// initialize csv writers
	writers := make(map[int]*csv.Writer)
	outputData := make(map[int]*bytes.Buffer)
	for index, colDR := range colsToFeaturize {
		output := &bytes.Buffer{}
		writer := csv.NewWriter(output)

		// write the header as needed
		if hasHeader {
			header := make([]string, len(colDR.newDataResource.Variables))
			for _, v := range colDR.newDataResource.Variables {
				header[v.Index] = v.Name
			}
			err = writer.Write(header)
			if err != nil {
				return errors.Wrap(err, "error writing header to output")
			}
		}
		writers[index] = writer
		outputData[index] = output
	}

	// skip header
	if hasHeader {
		_, err = reader.Read()
		if err != nil {
			return errors.Wrap(err, "failed to read header from file")
		}
	}

	log.Infof("reading data from source")
	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "failed to read line from file")
		}
		if count > 0 || !hasHeader {
			// featurize the row as necessary
			for index, colDR := range colsToFeaturize {
				imagePath := fmt.Sprintf("%s/%s", mediaPath, path.Join(colDR.originalResPath, line[index]))
				log.Infof("Featurizing %s", imagePath)
				feature, err := featurizeImage(imagePath, imageFeaturizer)
				if err != nil {
					return errors.Wrap(err, "error getting image feature output")
				}

				// write the index,feature output
				err = writers[index].Write([]string{line[d3mIndexFieldIndex], feature})
				if err != nil {
					return errors.Wrap(err, "error storing image feature output")
				}
			}
		}
		count++
	}

	// output the data
	log.Infof("Writing data to output")
	for index := range colsToFeaturize {
		writer := writers[index]
		pathToWrite := path.Join(outputPath, "features", "features.csv")
		writer.Flush()
		err = ioutil.WriteFile(pathToWrite, outputData[index].Bytes(), 0644)
		if err != nil {
			return errors.Wrap(err, "error writing image feature output")
		}
	}

	// output the schema
	log.Infof("Writing schema to output")
	err = meta.WriteSchema(path.Join(outputPath, "featureDatasetDoc.json"))

	return err
}

func addFeaturesToSchema(meta *metadata.Metadata, mainDR *metadata.DataResource) (int, map[int]*potentialFeature) {
	d3mIndexFieldIndex := -1
	colsToFeaturize := make(map[int]*potentialFeature)
	for _, v := range mainDR.Variables {
		if v.Name == metadata.D3MIndexName {
			d3mIndexFieldIndex = v.Index
		} else if v.RefersTo != nil && v.RefersTo["resID"] != nil {
			// get the refered DR
			resID := v.RefersTo["resID"].(string)

			res := getDataResource(meta, resID)

			// check if needs to be featurized
			if res.CanBeFeaturized() {
				// create the new resource to hold the featured output
				indexName := fmt.Sprintf("_feature_%s", v.Name)
				resIndex := fmt.Sprintf("%d", len(meta.DataResources))
				featureDR := &metadata.DataResource{
					ResID:        resIndex,
					ResPath:      "features/",
					ResType:      "table",
					IsCollection: false,
					Variables: []*metadata.Variable{
						{
							Name:  indexName,
							Index: 0,
							Type:  "integer",
							Role:  []string{"index"},
						},
						{
							Name:  "feature",
							Index: 1,
							Type:  "string",
							Role:  []string{"attribute"},
						},
					},
				}

				meta.DataResources = append(meta.DataResources, featureDR)

				// add a reference to the new data resource
				refData := map[string]interface{}{
					"refersTo": resIndex,
					"resObject": map[string]interface{}{
						"columnName": indexName,
					},
				}

				refVariable := &metadata.Variable{
					Name:     indexName,
					Index:    len(mainDR.Variables),
					Type:     "integer",
					Role:     []string{"key"},
					RefersTo: refData,
				}
				mainDR.Variables = append(mainDR.Variables, refVariable)

				colsToFeaturize[v.Index] = &potentialFeature{
					originalResPath: res.ResPath,
					newDataResource: featureDR,
				}
			}
		}
	}

	return d3mIndexFieldIndex, colsToFeaturize
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
		labelText = append(labelText, l.(string))
	}

	return strings.Join(labelText, " "), nil
}
