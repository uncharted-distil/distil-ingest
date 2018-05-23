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

	"github.com/jeffail/gabs"
	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/rest"
)

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
	for index := range colsToFeaturize {
		output := &bytes.Buffer{}
		writers[index] = csv.NewWriter(output)
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
				imagePath := fmt.Sprintf("%s/%s", mediaPath, path.Join(colDR.ResPath, line[index]))
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
		pathToWrite := path.Join(outputPath, "features.csv")
		writer.Flush()
		err = ioutil.WriteFile(pathToWrite, outputData[index].Bytes(), 0644)
		if err != nil {
			return errors.Wrap(err, "error writing image feature output")
		}
	}

	// output the schema
	if len(colsToFeaturize) > 0 {
		log.Infof("Writing schema to output")
		err = meta.WriteSchema(path.Join(outputPath, "datasetDoc.json"))
	}

	return err
}

func addFeaturesToSchema(meta *metadata.Metadata, mainDR *metadata.DataResource) (int, map[int]*metadata.DataResource) {
	d3mIndexFieldIndex := -1
	colsToFeaturize := make(map[int]*metadata.DataResource)
	for _, v := range mainDR.Variables {
		if v.Name == metadata.D3MIndexName {
			d3mIndexFieldIndex = v.Index
		} else if v.RefersTo != nil && v.RefersTo.Path("resID").Data() != nil {
			// get the refered DR
			resID := v.RefersTo.Path("resID").Data().(string)

			res := getDataResource(meta, resID)

			// check if needs to be featurized
			if res.CanBeFeaturized() {
				// create the new resource to hold the featured output
				indexName := fmt.Sprintf("_feature_%s", v.Name)
				resIndex := fmt.Sprintf("%d", len(meta.DataResources))
				featureDR := &metadata.DataResource{
					ResID:        resIndex,
					ResPath:      res.ResPath,
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

				colsToFeaturize[v.Index] = featureDR
				meta.DataResources = append(meta.DataResources, featureDR)

				// add a reference to the new data resource
				refData, _ := gabs.Consume(map[string]interface{}{
					"refersTo": resIndex,
					"resObject": map[string]interface{}{
						"columnName": resIndex,
					},
				})
				refVariable := &metadata.Variable{
					Name:     indexName,
					Index:    len(mainDR.Variables),
					Type:     "integer",
					Role:     []string{"key"},
					RefersTo: refData,
				}
				mainDR.Variables = append(mainDR.Variables, refVariable)
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
