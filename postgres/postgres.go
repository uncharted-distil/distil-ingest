package postgres

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"

	"github.com/go-pg/pg"
	"github.com/jeffail/gabs"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/deluge/document"
	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/postgres/model"
	"github.com/unchartedsoftware/distil-ingest/postgres/service"
	"github.com/unchartedsoftware/plog"
)

const (
	metadataTableCreationSQL = `CREATE TABLE %s (
			name	varchar(40)	NOT NULL,
			role	varchar(20),
			type	varchar(20)
		);`
	resultTableCreationSQL = `CREATE TABLE %s (
			result_id	varchar(1000)	NOT NULL,
			index		BIGINT,
			target		varchar(40),
			value		varchar(200)
		);`
)

var (
	nonNullableTypes = map[string]bool{
		"int":     true,
		"integer": true,
		"float":   true,
	}
)

// Database is a struct representing a full logical database.
type Database struct {
	DB             *pg.DB
	DatasetService *service.DatasetService
	Tables         map[string]*model.Dataset
}

// NewDatabase creates a new database instance.
func NewDatabase(config *conf.Conf) (*Database, error) {
	db := pg.Connect(&pg.Options{
		User:     config.DBUser,
		Password: config.DBPassword,
		Database: config.Database,
	})
	ds := service.NewDatasetService(db)

	database := &Database{
		DatasetService: ds,
		DB:             db,
		Tables:         make(map[string]*model.Dataset),
	}

	return database, nil
}

// CreateResultTable creates an empty table for the pipeline results.
func (d *Database) CreateResultTable(tableName string) error {
	resultTableName := fmt.Sprintf("%s_result", tableName)

	// Make sure the table is clear. If the table did not previously exist,
	// an error is returned. May as well ignore it since a serious problem
	// will cause errors on the other statements as well.
	err := d.DropTable(resultTableName)

	// Create the variable table.
	log.Infof("Creating result table %s", resultTableName)
	createStatement := fmt.Sprintf(resultTableCreationSQL, resultTableName)
	_, err = d.DB.Exec(createStatement)
	if err != nil {
		return err
	}

	return nil
}

// StoreMetadata stores the variable information to the specified table.
func (d *Database) StoreMetadata(tableName string) error {
	variableTableName := fmt.Sprintf("%s_variable", tableName)

	// Make sure the table is clear. If the table did not previously exist,
	// an error is returned. May as well ignore it since a serious problem
	// will cause errors on the other statements as well.
	err := d.DropTable(variableTableName)

	// Create the variable table.
	log.Infof("Creating variable table %s", variableTableName)
	createStatement := fmt.Sprintf(metadataTableCreationSQL, variableTableName)
	_, err = d.DB.Exec(createStatement)
	if err != nil {
		return err
	}

	// Insert the variable metadata into the new table.
	for _, v := range d.Tables[tableName].Variables {
		insertStatement := fmt.Sprintf("INSERT INTO %s (name, role, type) VALUES (?, ?, ?);", variableTableName)
		values := []interface{}{v.Name, v.Role, v.Type}
		_, err = d.DB.Exec(insertStatement, values...)
		if err != nil {
			return err
		}
	}

	return nil
}

// IngestRow parses the raw csv data and stores it to the table specified.
// The previously parsed metadata is used to map columns.
func (d *Database) IngestRow(tableName string, data string) error {
	ds := d.Tables[tableName]

	insertStatement := ""
	variables := ds.Variables
	values := make([]interface{}, len(variables))
	doc := &document.CSV{}
	doc.SetData(data)
	for i := 0; i < len(variables); i++ {
		// Ignore rows that have an empty column.
		if d.isNullVariable(variables[i].Type, doc.Cols[i]) {
			log.Warn(fmt.Sprintf("%s has empty value in record %s", variables[i].Name, data))
			return nil
		} else {
			insertStatement = fmt.Sprintf("%s, ?", insertStatement)

			// Map the raw string value to the correct database value.
			// Assume columns in metadata line up with columns in raw data.
			dbValue, err := d.mapVariable(variables[i].Type, doc.Cols[i])
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Failed to parse column %s", variables[i].Name))
			}
			values[i] = dbValue
		}
	}
	insertStatement = fmt.Sprintf("INSERT INTO %s VALUES (%s);", tableName, insertStatement[2:])

	_, err := d.DB.Exec(insertStatement, values...)

	return err
}

// DropTable drops the specified table from the database.
func (d *Database) DropTable(tableName string) error {
	log.Infof("Dropping table %s", tableName)
	drop := fmt.Sprintf("DROP TABLE %s;", tableName)
	_, err := d.DB.Exec(drop)
	log.Infof("Dropped table %s", tableName)

	return err
}

// InitializeTable generates and runs a table create statement based on the schema.
func (d *Database) InitializeTable(tableName string, ds *model.Dataset) error {
	d.Tables[tableName] = ds

	// Create the statement that will be used to create the table.
	createStatement := `CREATE TABLE %s(%s);`
	vars := ""
	for _, variable := range ds.Variables {
		vars = fmt.Sprintf("%s\n\"%s\" %s,", vars, variable.Name, d.mapType(variable.Type))
	}
	if len(vars) > 0 {
		vars = vars[:len(vars)-1]
	}
	createStatement = fmt.Sprintf(createStatement, tableName, vars)
	log.Infof("Creating table %s", tableName)

	// Create the table.
	_, err := d.DB.Exec(createStatement)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) InitializeDataset(meta *metadata.Metadata) (*model.Dataset, error) {
	ds := model.NewDataset(meta.ID, meta.Name, meta.Description, meta)

	return ds, nil
}

// ParseMetadata parses the schema information into a dataset.
func (d *Database) ParseMetadata(schemaPath string) (*model.Dataset, error) {
	// Unmarshall the schema file
	schema, err := gabs.ParseJSONFile(schemaPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parses schema file")
	}

	// load data description text
	descPath := schema.Path("descriptionFile").Data().(string)
	contents, err := ioutil.ReadFile(filepath.Dir(schemaPath) + "/" + descPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to load description file")
	}

	// create a new object for our output metadata and write the parts of the schema
	// we want into it - name, id, description, variable info
	dsId := schema.Path("datasetId").Data().(string)
	dsDesc := string(contents)
	dsName := ""
	val, ok := schema.Path("name").Data().(string)
	if ok {
		dsName = val
	}
	ds := model.NewDataset(dsId, dsName, dsDesc, nil)

	// add the training and target data variables. Ignore repeated columns.
	trainVariables, err := schema.Path("trainData.trainData").Children()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse training data")
	}
	targetVariables, err := schema.Path("trainData.trainTargets").Children()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse target data")
	}
	variables := append(trainVariables, targetVariables...)

	for _, variable := range variables {
		varName := variable.Path("varName").Data().(string)
		varRole := variable.Path("varRole").Data().(string)
		varType := variable.Path("varType").Data().(string)

		variable := metadata.NewVariable(varName, varType, varRole)
		if !ds.HasVariable(variable) {
			ds.AddVariable(variable)
		}
	}

	return ds, nil
}

func (d *Database) mapType(typ string) string {
	// NOTE: current classification has issues so if numerical, assume float64.
	switch typ {
	case "int":
		return "FLOAT8"
	case "integer":
		return "FLOAT8"
	case "float":
		return "FLOAT8"
	default:
		return "TEXT"
	}
}

// mapVariable uses the variable type to map a string value to the proper type.
func (d *Database) mapVariable(typ, value string) (interface{}, error) {
	// NOTE: current classification has issues so if numerical, assume float64.
	switch typ {
	case "int":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	case "integer":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	case "float":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}

func (d *Database) isNullVariable(typ, value string) bool {
	return value == "" && nonNullableTypes[typ]
}