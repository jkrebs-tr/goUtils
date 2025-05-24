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

// NewConnection creates a new NetSuite database connection using the provided connection string
//
// Parameters:
//   - connStr: The SQL Server connection string for NetSuite database
//
// Returns:
//   - *Connection: The database connection instance
//   - error: Any errors encountered during connection establishment
//
// Example Usage:
//
//	connStr := "sqlserver://username:password@localhost:1433?database=netsuite"
//	conn, err := NewConnection(connStr)
//	if err != nil {
//	    log.Fatal("Failed to connect to NetSuite database:", err)
//	}
//	
//	// Always remember to close the connection
//	defer conn.Close()
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

// Close closes the database connection and releases any associated resources
//
// Returns:
//   - error: Any errors encountered during connection closure
//
// Example Usage:
//
//	defer conn.Close()
//	
//	// Or explicitly close when done
//	if err := conn.Close(); err != nil {
//	    log.Printf("Error closing connection: %v", err)
//	}
func (c *Connection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Select executes a SQL query and scans the results into the provided destination slice
//
// Parameters:
//   - query: The SQL query string to execute
//   - dest: A pointer to a slice where query results will be stored
//   - args: Optional query arguments for parameterized queries
//
// Returns:
//   - error: Any errors encountered during query execution or result scanning
//
// Example Usage:
//
//	type Customer struct {
//	    ID    int    `db:"customer_id"`
//	    Name  string `db:"customer_name"`
//	    Email string `db:"email"`
//	}
//	
//	var customers []Customer
//	err := conn.Select("SELECT customer_id, customer_name, email FROM customers WHERE active = ?", &customers, 1)
//	if err != nil {
//	    log.Fatal("Query failed:", err)
//	}
//	
//	for _, customer := range customers {
//	    fmt.Printf("ID: %d, Name: %s, Email: %s\n", customer.ID, customer.Name, customer.Email)
//	}
//	
//	// With pointer slice
//	var customerPtrs []*Customer
//	err = conn.Select("SELECT customer_id, customer_name, email FROM customers WHERE region = ?", &customerPtrs, "US")
//	if err != nil {
//	    log.Fatal("Query with pointers failed:", err)
//	}
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
