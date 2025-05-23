package csv

import (
	"encoding/csv"
	"fmt"
	"os"
)

// CreateFile creates a new file in the current directory with any given headers (if provided)
//
// Parameters:
//   - fileName: The name of the file to be created
//   - headers: A string list of the values to be written in the header
//
// Returns:
// - *os.File: The file instance
// - *csv.Writer: The file Wrtier instance
// - error: Any errors
//
// Example Usage:
//
// file, writer, err := CreateFile("testing.csv", []string{"Col1", "Col2", "Col3"})
//
//	if err != nil {
//	    log.Fatal("File doesn't exist or can't be opened:", err)
//	}
//
//	// Always remember to close and flush
//	defer file.Close()
//	defer writer.Flush()
//
//	// Write data
//	writer.Write([]string{"New", "Data", "Row"})
func CreateFile(fileName string, headers []string) (*os.File, *csv.Writer, error) {
	var writer *csv.Writer

	file, err := os.Create(fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("Error Creating File (%s): %v", fileName, err)
	}

	_, err = file.Write([]byte{0xEF, 0xBB, 0xBF})
	if err != nil {
		return nil, nil, fmt.Errorf("Error writing UTF-8 BOM: %v", err)
	}

	if len(headers) > 0 {
		writer = csv.NewWriter(file)
		if err := writer.Write(headers); err != nil {
			return nil, nil, fmt.Errorf("Error writing header to file %s: %v", fileName, err)
		}
	}

	return file, writer, nil
}

// AppendFile opens an existing CSV file for appending.
//
// Parameters:
//   - fileName: The name of the existing file to be opened for appending
//
// Returns:
//   - *os.File: The file instance
//   - *csv.Writer: The CSV writer instance
//   - error: Any errors (including if file doesn't exist)
//
// Example Usage:
//
//	// Append to existing file
//	file, writer, err := AppendFile("existing.csv")
//	if err != nil {
//	    log.Fatal("File doesn't exist or can't be opened:", err)
//	}
//
//	// Always remember to close and flush
//	defer file.Close()
//	defer writer.Flush()
//
//	// Write data
//	writer.Write([]string{"New", "Data", "Row"})
func AppendFile(fileName string) (*os.File, *csv.Writer, error) {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("File does not exist: %s", fileName)
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("Error Opening File (%s): %v", fileName, err)
	}

	writer := csv.NewWriter(file)
	return file, writer, nil
}
