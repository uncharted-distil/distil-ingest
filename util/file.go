package util

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// CreateContainingDirs creates all directories on the supplied path.
func CreateContainingDirs(filePath string) error {
	dirToCreate := filepath.Dir(filePath)
	if dirToCreate != "/" && dirToCreate != "." {
		err := os.MkdirAll(dirToCreate, 0777)
		if err != nil {
			return errors.Wrap(err, "unable to create containing directory")
		}
	}

	return nil
}

// WriteFileWithDirs writes the file and creates any missing directories along
// the way.
func WriteFileWithDirs(filename string, data []byte, perm os.FileMode) error {

	dir, _ := filepath.Split(filename)

	// make all dirs up to the destination
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}

	// write the file
	return ioutil.WriteFile(filename, data, perm)
}

// DirExists checks to see if a directory exists.
func DirExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// FileExists checks to see if a file exists.
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}
