package bigquery

import (
    "context"
    "fmt"
    "reflect"

    "cloud.google.com/go/bigquery"
    "google.golang.org/api/iterator"
)

type Client struct {
    bq        *bigquery.Client
    projectID string
    ctx       context.Context
}

type QueryStats struct {
    RowsAffected int64
    JobID        string
}

type StreamingStats struct {
    RowsInserted int64
    Errors       []error
}

// NewClient creates a new BigQuery client instance with the specified project ID
//
// Parameters:
//   - ctx: The context for the client operations
//   - projectID: The Google Cloud project ID to use for BigQuery operations
//
// Returns:
//   - *Client: The BigQuery client instance
//   - error: Any errors encountered during client creation
//
// Example Usage:
//
//	ctx := context.Background()
//	client, err := NewClient(ctx, "my-project-id")
//	if err != nil {
//	    log.Fatal("Failed to create BigQuery client:", err)
//	}
//	
//	// Always remember to close the client
//	defer client.Close()
func NewClient(ctx context.Context, projectID string) (*Client, error) {
    bqClient, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
    }
    
    return &Client{
        bq:        bqClient,
        projectID: projectID,
        ctx:       ctx,
    }, nil
}

// Close closes the BigQuery client and releases any resources
//
// Returns:
//   - error: Any errors encountered during client closure
//
// Example Usage:
//
//	defer client.Close()
//	
//	// Or explicitly close when done
//	if err := client.Close(); err != nil {
//	    log.Printf("Error closing client: %v", err)
//	}
func (c *Client) Close() error {
    return c.bq.Close()
}

// StreamingInsert performs a streaming insert operation to insert rows into a BigQuery table
//
// Parameters:
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice or array of structs representing the rows to insert
//
// Returns:
//   - *StreamingStats: Statistics about the insert operation including rows inserted and any errors
//   - error: Any errors encountered during the streaming insert
//
// Example Usage:
//
//	type Person struct {
//	    Name string `bigquery:"name"`
//	    Age  int    `bigquery:"age"`
//	}
//	
//	people := []Person{
//	    {Name: "Alice", Age: 30},
//	    {Name: "Bob", Age: 25},
//	}
//	
//	stats, err := client.StreamingInsert("my_dataset", "people_table", people)
//	if err != nil {
//	    log.Fatal("Streaming insert failed:", err)
//	}
//	
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
func (c *Client) StreamingInsert(datasetID, tableID string, rows any) (*StreamingStats, error) {
    dataset := c.bq.Dataset(datasetID)
    table := dataset.Table(tableID)
    inserter := table.Inserter()
    
    rowsValue := reflect.ValueOf(rows)
    if rowsValue.Kind() != reflect.Slice && rowsValue.Kind() != reflect.Array {
        return nil, fmt.Errorf("rows must be a slice or array")
    }
    
    var bqRows []any
    for i := 0; i < rowsValue.Len(); i++ {
        bqRows = append(bqRows, rowsValue.Index(i).Interface())
    }
    
    err := inserter.Put(c.ctx, bqRows)
    if err != nil {
        return nil, fmt.Errorf("streaming insert failed: %w", err)
    }
    
    return &StreamingStats{
        RowsInserted: int64(len(bqRows)),
        Errors:       nil,
    }, nil
}

// StreamingInsertWithInsertIDs performs streaming insert with custom insert IDs for deduplication
//
// Parameters:
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice of bigquery.ValueSaver objects with custom insert IDs
//
// Returns:
//   - *StreamingStats: Statistics about the insert operation including rows inserted and any errors
//   - error: Any errors encountered during the streaming insert
//
// Example Usage:
//
//	type PersonWithID struct {
//	    InsertID string
//	    Name     string `bigquery:"name"`
//	    Age      int    `bigquery:"age"`
//	}
//	
//	func (p PersonWithID) Save() (map[string]bigquery.Value, string, error) {
//	    return map[string]bigquery.Value{
//	        "name": p.Name,
//	        "age":  p.Age,
//	    }, p.InsertID, nil
//	}
//	
//	people := []bigquery.ValueSaver{
//	    PersonWithID{InsertID: "unique-id-1", Name: "Alice", Age: 30},
//	    PersonWithID{InsertID: "unique-id-2", Name: "Bob", Age: 25},
//	}
//	
//	stats, err := client.StreamingInsertWithInsertIDs("my_dataset", "people_table", people)
//	if err != nil {
//	    log.Fatal("Streaming insert with IDs failed:", err)
//	}
//	
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
func (c *Client) StreamingInsertWithInsertIDs(datasetID, tableID string, rows []bigquery.ValueSaver) (*StreamingStats, error) {
    dataset := c.bq.Dataset(datasetID)
    table := dataset.Table(tableID)
    inserter := table.Inserter()
    
    err := inserter.Put(c.ctx, rows)
    if err != nil {
        return nil, fmt.Errorf("streaming insert with IDs failed: %w", err)
    }
    
    return &StreamingStats{
        RowsInserted: int64(len(rows)),
        Errors:       nil,
    }, nil
}

// StreamingInsertBatched performs streaming insert in batches for large datasets
//
// Parameters:
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice or array of structs representing the rows to insert
//   - batchSize: The number of rows to insert per batch (defaults to 1000 if <= 0)
//
// Returns:
//   - *StreamingStats: Statistics about the insert operation including total rows inserted and any batch errors
//   - error: Any errors encountered during the batched streaming insert
//
// Example Usage:
//
//	type Person struct {
//	    Name string `bigquery:"name"`
//	    Age  int    `bigquery:"age"`
//	}
//	
//	// Large dataset with 10,000 records
//	var people []Person
//	for i := 0; i < 10000; i++ {
//	    people = append(people, Person{Name: fmt.Sprintf("Person%d", i), Age: 20 + i%50})
//	}
//	
//	stats, err := client.StreamingInsertBatched("my_dataset", "people_table", people, 500)
//	if err != nil {
//	    log.Fatal("Batched streaming insert failed:", err)
//	}
//	
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
//	if len(stats.Errors) > 0 {
//	    fmt.Printf("Encountered %d batch errors\n", len(stats.Errors))
//	}
func (c *Client) StreamingInsertBatched(datasetID, tableID string, rows any, batchSize int) (*StreamingStats, error) {
    rowsValue := reflect.ValueOf(rows)
    if rowsValue.Kind() != reflect.Slice && rowsValue.Kind() != reflect.Array {
        return nil, fmt.Errorf("rows must be a slice or array")
    }
    
    totalRows := rowsValue.Len()
    if batchSize <= 0 {
        batchSize = 1000 // Default batch size
    }
    
    dataset := c.bq.Dataset(datasetID)
    table := dataset.Table(tableID)
    inserter := table.Inserter()
    
    totalInserted := int64(0)
    var allErrors []error
    
    for i := 0; i < totalRows; i += batchSize {
        end := i + batchSize
        if end > totalRows {
            end = totalRows
        }
        
        // Create batch
        var batch []any
        for j := i; j < end; j++ {
            batch = append(batch, rowsValue.Index(j).Interface())
        }
        
        err := inserter.Put(c.ctx, batch)
        if err != nil {
            allErrors = append(allErrors, fmt.Errorf("batch %d-%d failed: %w", i, end-1, err))
            continue
        }
        
        totalInserted += int64(len(batch))
    }
    
    return &StreamingStats{
        RowsInserted: totalInserted,
        Errors:       allErrors,
    }, nil
}

// Execute runs a BigQuery SQL query and returns statistics about the operation
//
// Parameters:
//   - sqlQuery: The SQL query string to execute
//   - params: Optional query parameters for parameterized queries
//
// Returns:
//   - *QueryStats: Statistics about the query execution including rows affected and job ID
//   - error: Any errors encountered during query execution
//
// Example Usage:
//
//	// Simple query
//	stats, err := client.Execute("UPDATE my_dataset.people_table SET age = age + 1 WHERE name = 'Alice'")
//	if err != nil {
//	    log.Fatal("Query execution failed:", err)
//	}
//	
//	fmt.Printf("Query %s affected %d rows\n", stats.JobID, stats.RowsAffected)
//	
//	// Parameterized query
//	param := bigquery.QueryParameter{
//	    Name:  "name_param",
//	    Value: "Bob",
//	}
//	
//	stats, err = client.Execute("DELETE FROM my_dataset.people_table WHERE name = @name_param", param)
//	if err != nil {
//	    log.Fatal("Parameterized query failed:", err)
//	}
func (c *Client) Execute(sqlQuery string, params ...bigquery.QueryParameter) (*QueryStats, error) {
    q := c.bq.Query(sqlQuery)
    
    if len(params) > 0 {
        q.Parameters = params
    }
    
    // Run the query
    job, err := q.Run(c.ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to start query job: %w", err)
    }
    
    // Wait for completion
    status, err := job.Wait(c.ctx)
    if err != nil {
        return nil, fmt.Errorf("job failed: %w", err)
    }
    
    if status.Err() != nil {
        return nil, fmt.Errorf("query job error: %w", status.Err())
    }
    
    // Get job statistics
    jobStats := status.Statistics
    var rowsAffected int64
    if dmlStats := jobStats.Details.(*bigquery.QueryStatistics); dmlStats != nil {
        rowsAffected = dmlStats.NumDMLAffectedRows
    }
    
    return &QueryStats{
        RowsAffected: rowsAffected,
        JobID:        job.ID(),
    }, nil
}

// Query executes a BigQuery SQL query and scans the results into the provided destination slice
//
// Parameters:
//   - sqlQuery: The SQL query string to execute
//   - dest: A pointer to a slice where query results will be stored
//   - params: Optional query parameters for parameterized queries
//
// Returns:
//   - error: Any errors encountered during query execution or result scanning
//
// Example Usage:
//
//	type Person struct {
//	    Name string `bigquery:"name"`
//	    Age  int    `bigquery:"age"`
//	}
//	
//	var people []Person
//	err := client.Query("SELECT name, age FROM my_dataset.people_table WHERE age > 25", &people)
//	if err != nil {
//	    log.Fatal("Query failed:", err)
//	}
//	
//	for _, person := range people {
//	    fmt.Printf("Name: %s, Age: %d\n", person.Name, person.Age)
//	}
//	
//	// Parameterized query
//	param := bigquery.QueryParameter{
//	    Name:  "min_age",
//	    Value: 30,
//	}
//	
//	var adults []Person
//	err = client.Query("SELECT name, age FROM my_dataset.people_table WHERE age >= @min_age", &adults, param)
//	if err != nil {
//	    log.Fatal("Parameterized query failed:", err)
//	}
func (c *Client) Query(sqlQuery string, dest any, params ...bigquery.QueryParameter) error {
    q := c.bq.Query(sqlQuery)
    
    if len(params) > 0 {
        q.Parameters = params
    }
    
    it, err := q.Read(c.ctx)
    if err != nil {
        return fmt.Errorf("query execution failed: %w", err)
    }
    
    destValue := reflect.ValueOf(dest)
    if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
        return fmt.Errorf("dest must be a pointer to a slice")
    }
    
    sliceValue := destValue.Elem()
    sliceType := sliceValue.Type()
    elementType := sliceType.Elem()
    
    for {
        elemPtr := reflect.New(elementType)
        elem := elemPtr.Interface()
        
        err := it.Next(elem)
        if err == iterator.Done {
            break
        }
        if err != nil {
            return fmt.Errorf("error reading row: %w", err)
        }
        
        sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(elem).Elem()))
    }
    
    return nil
}
