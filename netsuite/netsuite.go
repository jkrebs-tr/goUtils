package netsuite

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type Connection struct {
	db *sql.DB
}

func NewConnection(connStr string) (*Connection, error) {
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	return &Connection{db: db}, nil
}

func (c *Connection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Connection) Select(query string, dest any, args ...any) error {
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	sliceValue := destValue.Elem()
	sliceType := sliceValue.Type()
	elementType := sliceType.Elem()

	isPointer := elementType.Kind() == reflect.Ptr
	if isPointer {
		elementType = elementType.Elem()
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	for rows.Next() {
		var elemValue reflect.Value
		if isPointer {
			elemValue = reflect.New(elementType)
		} else {
			elemValue = reflect.New(elementType).Elem()
		}

		scanDests := make([]any, len(columns))
		elemStruct := elemValue
		if isPointer {
			elemStruct = elemValue.Elem()
		}

		for i, col := range columns {
			field := elemStruct.FieldByNameFunc(func(name string) bool {
				return strings.EqualFold(name, col)
			})

			if field.IsValid() && field.CanSet() {
				scanDests[i] = field.Addr().Interface()
			} else {
				var dummy any
				scanDests[i] = &dummy
			}
		}

		if err := rows.Scan(scanDests...); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		if isPointer {
			sliceValue.Set(reflect.Append(sliceValue, elemValue))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, elemValue))
		}
	}

	return rows.Err()
}
