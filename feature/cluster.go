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

package feature

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-ingest/metadata"
	"github.com/uncharted-distil/distil-ingest/rest"
)

// ClusterDataset clusters data based on referenced data resources
// in the metadata. The clusters are added as a variable in the metadata.
func ClusterDataset(meta *model.Metadata, imageFeaturizer *rest.Featurizer, sourcePath string, mediaPath string, outputFolder string, outputPathData string, outputPathSchema string, hasHeader bool) error {
	// find the main data resource
	mainDR := meta.GetMainDataResource()

	// cluster image columns
	log.Infof("adding clusters to schema")
	colsToFeaturize := addFeaturesToSchema(meta, mainDR, "_cluster_", "group")

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
		_, err = reader.Read()
		if err != nil {
			return errors.Wrap(err, "failed to read header from file")
		}
	}

	// build the list of files to submit for clustering
	files := make([]string, 0)
	lines := make([][]string, 0)
	log.Infof("reading data from source")
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "failed to read line from file")
		}
		lines = append(lines, line)

		// featurize the row as necessary
		for index, colDR := range colsToFeaturize {
			imagePath := fmt.Sprintf("%s/%s", mediaPath, path.Join(colDR.originalResPath, line[index]))
			files = append(files, imagePath)
		}
	}

	// cluster the files
	log.Infof("Clustering data with featurizer")
	clusteredImages, err := clusterImages(files, imageFeaturizer)
	if err != nil {
		return errors.Wrap(err, "failed to cluster images using featurizer")
	}

	// append and output the new clustered data
	log.Infof("Adding cluster labels to source data")
	for _, l := range lines {
		for index, colDR := range colsToFeaturize {
			imagePath := fmt.Sprintf("%s/%s", mediaPath, path.Join(colDR.originalResPath, l[index]))
			l = append(l, clusteredImages[imagePath])
		}

		err = writer.Write(l)
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
	err = metadata.WriteSchema(meta, schemaPathToWrite, true)

	return err
}

func clusterImages(filepaths []string, featurizer *rest.Featurizer) (map[string]string, error) {
	feature, err := featurizer.ClusterImages(filepaths)
	if err != nil {
		return nil, errors.Wrap(err, "failed to cluster images")
	}

	preds, ok := feature.Image["pred_class"].(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("image feature objects in unexpected format")
	}

	clusters := make(map[string]string)
	for i, c := range preds {
		index, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed parse file index")
		}
		cluster, ok := c.(float64)
		if !ok {
			return nil, errors.Errorf("failed to parse file cluster")
		}
		clusters[filepaths[index]] = strconv.Itoa(int(cluster))
	}

	return clusters, nil
}
