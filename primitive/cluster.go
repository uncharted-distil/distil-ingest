package primitive

import (
	"bytes"
	"encoding/csv"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// ClusterPrimitive will cluster the dataset fields using a primitive.
func (s *IngestStep) ClusterPrimitive(schemaFile string, dataset string,
	rootDataPath string, outputSchemaPath string, outputDataPath string, hasHeader bool) error {
	// create required folders for outputPath
	util.CreateContainingDirs(outputDataPath)
	util.CreateContainingDirs(outputSchemaPath)

	// load metadata from original schema
	meta, err := metadata.LoadMetadataFromOriginalSchema(schemaFile)
	if err != nil {
		return errors.Wrap(err, "unable to load original schema file")
	}
	mainDR := meta.GetMainDataResource()

	// add feature variables
	features, err := getClusterVariables(meta, "_cluster_")
	if err != nil {
		return errors.Wrap(err, "unable to get cluster variables")
	}

	d3mIndexField := getD3MIndexField(mainDR)

	// open the input file
	dataPath := path.Join(rootDataPath, mainDR.ResPath)
	lines, err := s.readCSVFile(dataPath, hasHeader)
	if err != nil {
		return errors.Wrap(err, "error reading raw data")
	}

	// add the cluster data to the raw data
	for _, f := range features {
		mainDR.Variables = append(mainDR.Variables, f.Variable)

		lines, err = s.appendFeature(dataset, d3mIndexField, hasHeader, f, lines)
		if err != nil {
			return errors.Wrap(err, "error appending clustered data")
		}
	}

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output the header
	header := make([]string, len(mainDR.Variables))
	for _, v := range mainDR.Variables {
		header[v.Index] = v.Name
	}
	err = writer.Write(header)
	if err != nil {
		return errors.Wrap(err, "error storing clustered header")
	}

	for _, line := range lines {
		err = writer.Write(line)
		if err != nil {
			return errors.Wrap(err, "error storing clustered output")
		}
	}

	// output the data with the new feature
	writer.Flush()

	err = util.WriteFileWithDirs(outputDataPath, output.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error writing clustered output")
	}

	relativePath := getRelativePath(rootDataPath, outputDataPath)
	mainDR.ResPath = relativePath

	// write the new schema to file
	err = meta.WriteSchema(outputSchemaPath)
	if err != nil {
		return errors.Wrap(err, "unable to store cluster schema")
	}

	return nil
}
