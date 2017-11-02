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

	sessionTableName        = "session"
	requestTableName        = "request"
	resultMetadataTableName = "result"
	resultScoreTableName    = "result_score"
	requestFeatureTableName = "request_feature"

	sessionTableCreationSQL = `CREATE TABLE %s (
			session_id	varchar(200)
		);`
	requestTableCreationSQL = `CREATE TABLE %s (
			session_id			varchar(200),
			request_id			varchar(200),
			dataset				varchar(200),
			progress			varchar(40),
			created_time		timestamp,
			last_updated_time	timestamp
		);`
	resultMetadataTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			pipeline_id		varchar(200),
			result_uuid		varchar(200),
			result_uri		varchar(200),
			progress		varchar(40),
			output_type		varchar(200),
			created_time	timestamp
		);`
	requestFeatureTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			feature_name	varchar(40),
			feature_type	varchar(20)
		);`
	resultScoreTableCreationSQL = `CREATE TABLE %s (
			pipeline_id	varchar(200),
			metric		varchar(40),
			score		double precision
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
	DB     *pg.DB
	Tables map[string]*model.Dataset
}

// NewDatabase creates a new database instance.
func NewDatabase(config *conf.Conf) (*Database, error) {
	db := pg.Connect(&pg.Options{
		User:     config.DBUser,
		Password: config.DBPassword,
		Database: config.Database,
	})

	database := &Database{
		DB:     db,
		Tables: make(map[string]*model.Dataset),
	}

	return database, nil
}

// CreatePipelineMetadataTables creates an empty table for the pipeline results.
func (d *Database) CreatePipelineMetadataTables() error {
	// Create the pipeline tables.
	log.Infof("Creating pipeline metadata tables.")
	d.DropTable(sessionTableName)
	_, err := d.DB.Exec(fmt.Sprintf(sessionTableCreationSQL, sessionTableName))
	if err != nil {
		return err
	}

	d.DropTable(requestTableName)
	_, err = d.DB.Exec(fmt.Sprintf(requestTableCreationSQL, requestTableName))
	if err != nil {
		return err
	}

	d.DropTable(resultMetadataTableName)
	_, err = d.DB.Exec(fmt.Sprintf(resultMetadataTableCreationSQL, resultMetadataTableName))
	if err != nil {
		return err
	}

	d.DropTable(requestFeatureTableName)
	_, err = d.DB.Exec(fmt.Sprintf(requestFeatureTableCreationSQL, requestFeatureTableName))
	if err != nil {
		return err
	}

	d.DropTable(resultScoreTableName)
	_, err = d.DB.Exec(fmt.Sprintf(resultScoreTableCreationSQL, resultScoreTableName))
	if err != nil {
		return err
	}

	return nil
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
		// Default columns that have an empty column.
		var val interface{}
		if d.isNullVariable(variables[i].Type, doc.Cols[i]) {
			val = nil
		} else {
			val = doc.Cols[i]
		}
		insertStatement = fmt.Sprintf("%s, ?", insertStatement)
		values[i] = val
	}
	insertStatement = fmt.Sprintf("INSERT INTO %s_base VALUES (%s);", tableName, insertStatement[2:])

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

	// Create the view and table statements.
	// The table has everything stored as a string.
	// The view uses casting to set the types.
	createStatementTable := `CREATE TABLE %s_base (%s);`
	createStatementView := `CREATE VIEW %s AS SELECT %s FROM %s_base;`
	varsTable := ""
	varsView := ""
	for _, variable := range ds.Variables {
		varsTable = fmt.Sprintf("%s\n\"%s\" TEXT,", varsTable, variable.Name)
		varsView = fmt.Sprintf("%s\nCOALESCE(CAST(\"%s\" AS %s), %v) AS \"%s\",",
			varsView, variable.Name, d.mapType(variable.Type), d.defaultValue(variable.Type), variable.Name)
	}
	if len(varsTable) > 0 {
		varsTable = varsTable[:len(varsTable)-1]
		varsView = varsView[:len(varsView)-1]
	}
	createStatementTable = fmt.Sprintf(createStatementTable, tableName, varsTable)
	log.Infof("Creating table %s", tableName)

	// Create the table.
	_, err := d.DB.Exec(createStatementTable)
	if err != nil {
		return err
	}

	createStatementView = fmt.Sprintf(createStatementView, tableName, varsView, tableName)
	log.Infof("Creating view %s", tableName)

	// Create the table.
	_, err = d.DB.Exec(createStatementView)
	if err != nil {
		return err
	}

	return nil
}

// InitializeDataset initializes the dataset with the provided metadata.
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
	dsID := schema.Path("datasetId").Data().(string)
	dsDesc := string(contents)
	dsName := ""
	val, ok := schema.Path("name").Data().(string)
	if ok {
		dsName = val
	}
	ds := model.NewDataset(dsID, dsName, dsDesc, nil)

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
		return "INTEGER"
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
		return strconv.ParseInt(value, 10, 32)
	case "float":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}

func (d *Database) defaultValue(typ string) interface{} {
	switch typ {
	case "int":
		return float64(0)
	case "integer":
		return int(0)
	case "float":
		return float64(0)
	default:
		return "''"
	}
}

func (d *Database) isNullVariable(typ, value string) bool {
	return value == "" && nonNullableTypes[typ]
}
