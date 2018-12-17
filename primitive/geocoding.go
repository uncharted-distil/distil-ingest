package primitive

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/otiai10/copy"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/distil-compute/model"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/description"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/result"

	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// GeocodedPoint contains data that has been geocoded.
type GeocodedPoint struct {
	D3MIndex    string
	SourceField string
	Latitude    float64
	Longitude   float64
}

// GeocodeForwardUpdate will geocode location columns into lat & lon values
// and output the combined data to disk.
func (s *IngestStep) GeocodeForwardUpdate(schemaFile string, classificationPath string,
	dataset string, rootDataPath string, outputFolder string, hasHeader bool) error {
	outputSchemaPath := path.Join(outputFolder, D3MSchemaPathRelative)
	outputDataPath := path.Join(outputFolder, D3MDataPathRelative)
	sourceFolder := path.Dir(dataset)

	// copy the source folder to have all the linked files for merging
	err := copy.Copy(sourceFolder, outputFolder)
	if err != nil {
		return errors.Wrap(err, "unable to copy source data")
	}

	// delete the existing files that will be overwritten
	os.Remove(outputSchemaPath)
	os.Remove(outputDataPath)
	// load metadata from original schema
	meta, err := metadata.LoadMetadataFromClassification(schemaFile, classificationPath)
	if err != nil {
		return errors.Wrap(err, "unable to load original schema file")
	}
	mainDR := meta.GetMainDataResource()
	geocodedData, err := s.GeocodeForward(meta, dataset)
	if err != nil {
		return err
	}

	// map geocoded data by d3m index
	indexedData := make(map[string][]*GeocodedPoint)
	fields := make(map[string][]*model.Variable)
	for _, field := range geocodedData {
		latName, lonName := getLatLonVariableNames(field[0].SourceField)
		fields[field[0].SourceField] = []*model.Variable{
			model.NewVariable(len(mainDR.Variables), latName, "label", latName, "string", "string", []string{"attribute"}, model.VarRoleMetadata, nil, mainDR.Variables, false),
			model.NewVariable(len(mainDR.Variables)+1, lonName, "label", lonName, "string", "string", []string{"attribute"}, model.VarRoleMetadata, nil, mainDR.Variables, false),
		}
		mainDR.Variables = append(mainDR.Variables, fields[field[0].SourceField][0])
		mainDR.Variables = append(mainDR.Variables, fields[field[0].SourceField][1])
		for _, gc := range field {
			if indexedData[gc.D3MIndex] == nil {
				indexedData[gc.D3MIndex] = make([]*GeocodedPoint, 0)
			}
			indexedData[gc.D3MIndex] = append(indexedData[gc.D3MIndex], gc)
		}
	}

	// read raw data
	dataPath := path.Join(rootDataPath, mainDR.ResPath)
	lines, err := s.readCSVFile(dataPath, hasHeader)
	if err != nil {
		return errors.Wrap(err, "error reading raw data")
	}

	// add the geocoded data to the raw data
	d3mIndexVariable := getD3MIndexField(mainDR)
	for _, line := range lines {
		geocodedFields := indexedData[line[d3mIndexVariable]]
		for _, geo := range geocodedFields {
			line = append(line, fmt.Sprintf("%f", geo.Latitude))
			line = append(line, fmt.Sprintf("%f", geo.Longitude))
		}
		if err != nil {
			return errors.Wrap(err, "error appending feature data")
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
		return errors.Wrap(err, "error storing feature header")
	}

	// output the data with the new feature
	writer.Flush()
	err = util.WriteFileWithDirs(outputDataPath, output.Bytes(), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error writing feature output")
	}

	relativePath := getRelativePath(path.Dir(outputSchemaPath), outputDataPath)
	mainDR.ResPath = relativePath

	// write the new schema to file
	err = metadata.WriteSchema(meta, outputSchemaPath)
	if err != nil {
		return errors.Wrap(err, "unable to store feature schema")
	}

	return nil
}

func getLatLonVariableNames(variableName string) (string, string) {
	lat := fmt.Sprintf("_lat_%s", variableName)
	lon := fmt.Sprintf("_lon_%s", variableName)

	return lat, lon
}

// GeocodeForward will geocode location columns into lat & lon values.
func (s *IngestStep) GeocodeForward(meta *model.Metadata, dataset string) ([][]*GeocodedPoint, error) {
	// check to see if Simon typed something as a place.
	colsToGeocode := geocodeColumns(meta)
	geocodedFields := make([][]*GeocodedPoint, 0)
	datasetFolder := path.Dir(dataset)

	// cycle through the columns to geocode
	for _, col := range colsToGeocode {
		// create & submit the solution request
		pip, err := description.CreateGoatForwardPipeline("mountain", "", col, meta.DataResources[0].Variables)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create Goat pipeline")
		}

		datasetURI, err := s.submitPrimitive(datasetFolder, pip)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run Goat pipeline")
		}

		// parse primitive response
		res, err := result.ParseResultCSV(datasetURI)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse Goat pipeline result")
		}

		// result should be d3m index, lat, lon
		geocodedData := make([]*GeocodedPoint, len(res)-1)
		for i, v := range res {
			if i > 0 {
				d3mIndex, ok := v[0].(string)
				if !ok {
					return nil, errors.Errorf("unable to parse d3m index from result")
				}
				lat, err := strconv.ParseFloat(v[1].(string), 64)
				if err != nil {
					return nil, errors.Wrap(err, "unable to parse latitude from result")
				}
				lon, err := strconv.ParseFloat(v[2].(string), 64)
				if err != nil {
					return nil, errors.Wrap(err, "unable to parse longitude from result")
				}

				geocodedData = append(geocodedData, &GeocodedPoint{
					D3MIndex:    d3mIndex,
					SourceField: col,
					Latitude:    lat,
					Longitude:   lon,
				})
			}
		}

		geocodedFields = append(geocodedFields, geocodedData)
	}

	return geocodedFields, nil
}

func geocodeColumns(meta *model.Metadata) []string {
	// cycle throught types to determine columns to geocode.
	colsToGeocode := make([]string, 0)
	for _, v := range meta.DataResources[0].Variables {
		for _, t := range v.SuggestedTypes {
			if isLocationType(t.Type) {
				colsToGeocode = append(colsToGeocode, v.Name)
			}
		}
	}

	return colsToGeocode
}

func isLocationType(typ string) bool {
	return typ == model.AddressType || typ == model.CityType || typ == model.CountryType ||
		typ == model.PostalCodeType || typ == model.StateType || typ == model.TA2LocationType
}
