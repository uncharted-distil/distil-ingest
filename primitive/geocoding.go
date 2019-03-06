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

	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-compute/primitive/compute/description"
	"github.com/uncharted-distil/distil-compute/primitive/compute/result"

	"github.com/uncharted-distil/distil-ingest/metadata"
	"github.com/uncharted-distil/distil-ingest/util"
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
	meta, err := metadata.LoadMetadataFromClassification(schemaFile, classificationPath, false)
	if err != nil {
		return errors.Wrap(err, "unable to load original schema file")
	}
	mainDR := meta.GetMainDataResource()
	d3mIndexVariable := getD3MIndexField(mainDR)

	// read raw data
	dataPath := path.Join(rootDataPath, mainDR.ResPath)
	lines, err := s.readCSVFile(dataPath, hasHeader)
	if err != nil {
		return errors.Wrap(err, "error reading raw data")
	}

	// Geocode location fields
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
		mainDR.Variables = append(mainDR.Variables, fields[field[0].SourceField]...)
		for _, gc := range field {
			if indexedData[gc.D3MIndex] == nil {
				indexedData[gc.D3MIndex] = make([]*GeocodedPoint, 0)
			}
			indexedData[gc.D3MIndex] = append(indexedData[gc.D3MIndex], gc)
		}
	}

	// add the geocoded data to the raw data
	for i, line := range lines {
		geocodedFields := indexedData[line[d3mIndexVariable]]
		for _, geo := range geocodedFields {
			line = append(line, fmt.Sprintf("%f", geo.Latitude))
			line = append(line, fmt.Sprintf("%f", geo.Longitude))
		}
		lines[i] = line
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

	for _, line := range lines {
		err = writer.Write(line)
		if err != nil {
			return errors.Wrap(err, "error storing geocoded output")
		}
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
		pip, err := description.CreateGoatForwardPipeline("mountain", "", col)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create Goat pipeline")
		}

		datasetURI, err := s.submitPrimitive([]string{datasetFolder}, pip)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run Goat pipeline")
		}

		// parse primitive response
		res, err := result.ParseResultCSV(datasetURI)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse Goat pipeline result")
		}

		// result should be row index, input data, <col>_lat, <col>_lon
		// pull the d3m index as well as the lat & lon
		geocodedData := make([]*GeocodedPoint, len(res)-1)
		header := toStringArray(res[0])
		latIndex := getFieldIndex(header, fmt.Sprintf("%s_latitude", col))
		lonIndex := getFieldIndex(header, fmt.Sprintf("%s_longitude", col))
		d3mIndexIndex := getFieldIndex(header, model.D3MIndexName)
		for i, v := range res[1:] {
			lat, err := strconv.ParseFloat(v[latIndex].(string), 64)
			if err != nil {
				return nil, errors.Wrap(err, "unable to parse latitude from result")
			}
			lon, err := strconv.ParseFloat(v[lonIndex].(string), 64)
			if err != nil {
				return nil, errors.Wrap(err, "unable to parse longitude from result")
			}

			d3mIndex := v[d3mIndexIndex].(string)

			geocodedData[i-1] = &GeocodedPoint{
				D3MIndex:    d3mIndex,
				SourceField: col,
				Latitude:    lat,
				Longitude:   lon,
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
