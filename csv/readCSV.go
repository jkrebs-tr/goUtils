package csv

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

func ReadCSV[T any](fileName string, result *T) ([]*T, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("Error Opening File (%s): %v", fileName, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("Error Reading Header: %v", err)
	}

	// Clean headers
	for i := range headers {
		headers[i] = strings.TrimSpace(strings.TrimPrefix(headers[i], "\ufeff"))
	}

	rowChan := make(chan []string, 100)
	resultChan := make(chan parseResult[T], 100)

	// Worker pool
	numWorkers := runtime.NumCPU() * 2
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for record := range rowChan {
				res := processRow[T](record, headers)
				resultChan <- res
			}
		}()
	}

	// Reader goroutine
	go func() {
		for {
			record, err := reader.Read()
			if err != nil {
				break
			}
			rowChan <- record
		}
		close(rowChan)
	}()

	// Collector goroutine
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var results []*T
	var errors []error

	for res := range resultChan {
		if res.err != nil {
			errors = append(errors, res.err)
			continue
		}
		result := res.res
		results = append(results, &result)
	}

	if len(errors) > 0 {
		return results, fmt.Errorf("encountered %d errors during parsing", len(errors))
	}

	return results, nil
}

func processRow[T any](record []string, headers []string) parseResult[T] {
	var result parseResult[T]

	if len(record) < len(headers) {
		padding := make([]string, len(headers)-len(record))
		record = append(record, padding...)
	}

	fieldMap := make(map[string]string)
	for j, header := range headers {
		if j >= len(record) {
			continue
		}
		key := strings.TrimSpace(strings.TrimPrefix(header, "\ufeff"))
		val := strings.ReplaceAll(record[j], "\n", " ")
		val = strings.ReplaceAll(val, "\r", " ")
		val = strings.ReplaceAll(val, "\"\"", "\"")
		fieldMap[key] = val
	}

	// Convert map to struct
	err := mapToStruct(fieldMap, &result.res)
	if err != nil {
		result.err = err
		result.raw = fmt.Sprintf("%v", record)
	}

	return result
}

// Helper function to convert map to struct
func mapToStruct(m map[string]string, v interface{}) error {
	rv := reflect.ValueOf(v).Elem()
	rt := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		fieldType := rt.Field(i)

		csvTag := fieldType.Tag.Get("csv")
		if csvTag == "" {
			csvTag = fieldType.Name
		}

		if csvTag == "-" {
			continue
		}

		value, exists := m[csvTag]
		if !exists {
			continue
		}

		if err := setFieldValue(field, value); err != nil {
			return fmt.Errorf("error setting field %s: %v", fieldType.Name, err)
		}
	}

	return nil
}

// Helper function to set field values based on type
func setFieldValue(field reflect.Value, value string) error {
	if !field.CanSet() {
		return fmt.Errorf("cannot set field")
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value == "" {
			return nil
		}
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value == "" {
			return nil
		}
		u, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(u)
	case reflect.Float32, reflect.Float64:
		if value == "" {
			return nil
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		field.SetFloat(f)
	case reflect.Bool:
		if value == "" {
			return nil
		}
		b, err := strconv.ParseBool(value)
		if err != nil {
			switch strings.ToLower(value) {
			case "yes", "y", "1", "true":
				field.SetBool(true)
			case "no", "n", "0", "false":
				field.SetBool(false)
			default:
				return err
			}
		} else {
			field.SetBool(b)
		}
	case reflect.Ptr:
		if value == "" {
			return nil
		}
		newPtr := reflect.New(field.Type().Elem())
		if err := setFieldValue(newPtr.Elem(), value); err != nil {
			return err
		}
		field.Set(newPtr)
	default:
		if err := json.Unmarshal([]byte(value), field.Addr().Interface()); err != nil {
			return fmt.Errorf("unsupported field type: %v", field.Kind())
		}
	}

	return nil
}
