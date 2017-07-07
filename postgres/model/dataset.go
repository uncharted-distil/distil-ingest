package model

import (
	"strconv"
)

// Dataset is a struct containing the metadata of a dataset being processed.
type Dataset struct {
	ID              string
	Name            string
	Description     string
	Variables       []*Variable
	variablesLookup map[string]bool
}

// Variable is a struct representing a column.
type Variable struct {
	Name        string
	Description string
	Role        string
	Type        string
	Dataset     *Dataset
}

// NewDataset creates a new dataset instance.
func NewDataset(id, name, description string) *Dataset {
	return &Dataset{
		ID:              id,
		Name:            name,
		Description:     description,
		Variables:       make([]*Variable, 0),
		variablesLookup: make(map[string]bool),
	}
}

// HasVariable checks to see if a variable is already contained in the dataset.
func (ds *Dataset) HasVariable(variable *Variable) bool {
	return ds.variablesLookup[variable.Name]
}

// AddVariable adds a variable to the dataset.
func (ds *Dataset) AddVariable(variable *Variable) {
	ds.Variables = append(ds.Variables, variable)
	ds.variablesLookup[variable.Name] = true
}

// NewVariable creates a new variable instance.
func NewVariable(name, description, role, typ string, dataset *Dataset) *Variable {
	return &Variable{
		Name:        name,
		Description: description,
		Role:        role,
		Type:        typ,
		Dataset:     dataset,
	}
}

// MapType uses the variable type to map a string value to the proper type.
func (v *Variable) MapType(value string) (interface{}, error) {
	switch v.Type {
	case "BIGINT":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseInt(value, 10, 64)
	case "FLOAT8":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}
