package bigquery

import (
	"context"
	"strings"
	"time"
	"fmt"

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

// StreamingInsert performs a streaming insert operation to insert typed rows into a BigQuery table
//
// Parameters:
//   - c: The BigQuery client instance
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice of structs of type T representing the rows to insert
//
// Returns:
//   - *StreamingStats: Statistics about the insert operation including rows inserted
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
//	stats, err := StreamingInsert(client, "my_dataset", "people_table", people)
//	if err != nil {
//	    log.Fatal("Streaming insert failed:", err)
//	}
//
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
func StreamingInsert[T any](c *Client, datasetID, tableID string, rows []T) (*StreamingStats, error) {
	dataset := c.bq.Dataset(datasetID)
	table := dataset.Table(tableID)
	inserter := table.Inserter()

	bqRows := make([]any, len(rows))
	for i, r := range rows {
		bqRows[i] = r
	}

	if err := inserter.Put(c.ctx, bqRows); err != nil {
		return nil, fmt.Errorf("streaming insert failed: %w", err)
	}
	return &StreamingStats{RowsInserted: int64(len(bqRows))}, nil
}

// StreamingInsertWithInsertIDs performs streaming insert with custom insert IDs for deduplication using typed rows
//
// Parameters:
//   - c: The BigQuery client instance
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice of bigquery.ValueSaver objects of type T with custom insert IDs
//
// Returns:
//   - *StreamingStats: Statistics about the insert operation including rows inserted
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
//	people := []PersonWithID{
//	    {InsertID: "unique-id-1", Name: "Alice", Age: 30},
//	    {InsertID: "unique-id-2", Name: "Bob", Age: 25},
//	}
//
//	stats, err := StreamingInsertWithInsertIDs(client, "my_dataset", "people_table", people)
//	if err != nil {
//	    log.Fatal("Streaming insert with IDs failed:", err)
//	}
//
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
func StreamingInsertWithInsertIDs[T bigquery.ValueSaver](c *Client, datasetID, tableID string, rows []T) (*StreamingStats, error) {
	dataset := c.bq.Dataset(datasetID)
	table := dataset.Table(tableID)
	inserter := table.Inserter()

	bqRows := make([]bigquery.ValueSaver, len(rows))
	for i, r := range rows {
		bqRows[i] = r
	}

	if err := inserter.Put(c.ctx, bqRows); err != nil {
		return nil, fmt.Errorf("streaming insert with IDs failed: %w", err)
	}

	return &StreamingStats{
		RowsInserted: int64(len(rows)),
	}, nil
}

// StreamingInsertBatched performs streaming insert in batches for large datasets using typed rows
//
// Parameters:
//   - c: The BigQuery client instance
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert data into
//   - rows: A slice of structs of type T representing the rows to insert
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
//	stats, err := StreamingInsertBatched(client, "my_dataset", "people_table", people, 500)
//	if err != nil {
//	    log.Fatal("Batched streaming insert failed:", err)
//	}
//
//	fmt.Printf("Inserted %d rows\n", stats.RowsInserted)
//	if len(stats.Errors) > 0 {
//	    fmt.Printf("Encountered %d batch errors\n", len(stats.Errors))
//	}
func StreamingInsertBatched[T any](c *Client, datasetID, tableID string, rows []T, batchSize int) (*StreamingStats, error) {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	dataset := c.bq.Dataset(datasetID)
	table := dataset.Table(tableID)
	inserter := table.Inserter()

	totalInserted := int64(0)
	var allErrors []error

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		// Create batch
		batch := make([]any, end-i)
		for j := i; j < end; j++ {
			batch[j-i] = rows[j]
		}

		if err := inserter.Put(c.ctx, batch); err != nil {
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

// Query executes a BigQuery SQL query and scans the results into the provided destination slice using typed results
//
// Parameters:
//   - c: The BigQuery client instance
//   - sqlQuery: The SQL query string to execute
//   - dest: A pointer to a slice of type T where query results will be stored
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
//	err := Query(client, "SELECT name, age FROM my_dataset.people_table WHERE age > 25", &people)
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
//	err = Query(client, "SELECT name, age FROM my_dataset.people_table WHERE age >= @min_age", &adults, param)
//	if err != nil {
//	    log.Fatal("Parameterized query failed:", err)
//	}
func Query[T any](c *Client, sqlQuery string, dest *[]T, params ...bigquery.QueryParameter) error {
	q := c.bq.Query(sqlQuery)

	if len(params) > 0 {
		q.Parameters = params
	}

	it, err := q.Read(c.ctx)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	for {
		var elem T
		err := it.Next(&elem)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading row: %w", err)
		}

		*dest = append(*dest, elem)
	}

	return nil
}

// InsertOrUpdate performs an upsert operation using BigQuery's MERGE statement
// It will update existing rows based on unique fields or insert new rows if they don't exist
//
// Parameters:
//   - c: The BigQuery client instance
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert/update data
//   - rows: A slice of structs of type T representing the rows to upsert
//   - uniqueFields: Field names that should be used to match existing rows (e.g., ["id", "email"])
//
// Returns:
//   - *QueryStats: Statistics about the operation including rows affected and job ID
//   - error: Any errors encountered during the upsert operation
//
// Example Usage:
//
//	type Person struct {
//	    ID    int    `bigquery:"id"`
//	    Name  string `bigquery:"name"`
//	    Email string `bigquery:"email"`
//	    Age   int    `bigquery:"age"`
//	}
//
//	people := []Person{
//	    {ID: 1, Name: "Alice Smith", Email: "alice@example.com", Age: 31},
//	    {ID: 2, Name: "Bob Jones", Email: "bob@example.com", Age: 26},
//	    {ID: 3, Name: "Charlie Brown", Email: "charlie@example.com", Age: 35},
//	}
//
//	// Use ID as the unique field for matching
//	stats, err := InsertOrUpdate(client, "my_dataset", "people_table", people, []string{"id"})
//	if err != nil {
//	    log.Fatal("Insert or update failed:", err)
//	}
//
//	fmt.Printf("Affected %d rows, Job ID: %s\n", stats.RowsAffected, stats.JobID)
//
//	// Use multiple fields for uniqueness (composite key)
//	stats, err = InsertOrUpdate(client, "my_dataset", "people_table", people, []string{"email", "name"})
//	if err != nil {
//	    log.Fatal("Insert or update failed:", err)
//	}
func InsertOrUpdate[T any](c *Client, datasetID, tableID string, rows []T, uniqueFields []string) (*QueryStats, error) {
	if len(rows) == 0 {
		return &QueryStats{RowsAffected: 0}, nil
	}
	
	if len(uniqueFields) == 0 {
		return nil, fmt.Errorf("at least one unique field must be specified")
	}

	tempTableID := fmt.Sprintf("%s_temp_%d", tableID, time.Now().Unix())
	
	dataset := c.bq.Dataset(datasetID)
	targetTable := dataset.Table(tableID)
	tempTable := dataset.Table(tempTableID)
	
	meta, err := targetTable.Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get target table metadata: %w", err)
	}
	
	if err := tempTable.Create(c.ctx, &bigquery.TableMetadata{
		Schema:         meta.Schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		return nil, fmt.Errorf("failed to create temporary table: %w", err)
	}
	
	defer func() {
		if deleteErr := tempTable.Delete(c.ctx); deleteErr != nil {
			fmt.Printf("Warning: failed to delete temporary table %s: %v\n", tempTableID, deleteErr)
		}
	}()
	
	inserter := tempTable.Inserter()
	bqRows := make([]any, len(rows))
	for i, r := range rows {
		bqRows[i] = r
	}
	
	if err := inserter.Put(c.ctx, bqRows); err != nil {
		return nil, fmt.Errorf("failed to insert data into temporary table: %w", err)
	}
	
	mergeSQL := c.buildMergeSQL(datasetID, tableID, tempTableID, uniqueFields, meta.Schema)
	
	q := c.bq.Query(mergeSQL)
	job, err := q.Run(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run merge query: %w", err)
	}
	
	status, err := job.Wait(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("merge job failed: %w", err)
	}
	
	if status.Err() != nil {
		return nil, fmt.Errorf("merge job completed with error: %w", status.Err())
	}
	
	jobStats := job.LastStatus().Statistics
	var rowsAffected int64
	if dmlStats := jobStats.Details.(*bigquery.QueryStatistics); dmlStats != nil {
		rowsAffected = dmlStats.NumDMLAffectedRows
	}
	
	return &QueryStats{
		RowsAffected: rowsAffected,
		JobID:        job.ID(),
	}, nil
}

func (c *Client) buildMergeSQL(datasetID, targetTableID, sourceTableID string, uniqueFields []string, schema bigquery.Schema) string {
	target := fmt.Sprintf("`%s.%s.%s`", c.projectID, datasetID, targetTableID)
	source := fmt.Sprintf("`%s.%s.%s`", c.projectID, datasetID, sourceTableID)
	
	var onConditions []string
	for _, field := range uniqueFields {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", field, field))
	}
	onClause := strings.Join(onConditions, " AND ")
	
	var allFields []string
	var updateAssignments []string
	
	for _, field := range schema {
		fieldName := field.Name
		allFields = append(allFields, fieldName)
		
		isUniqueField := false
		for _, uf := range uniqueFields {
			if uf == fieldName {
				isUniqueField = true
				break
			}
		}
		
		if !isUniqueField {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = source.%s", fieldName, fieldName))
		}
	}
	
	insertFields := strings.Join(allFields, ", ")
	insertValues := strings.Join(func() []string {
		var values []string
		for _, field := range allFields {
			values = append(values, "source."+field)
		}
		return values
	}(), ", ")
	
	updateClause := ""
	if len(updateAssignments) > 0 {
		updateClause = "SET " + strings.Join(updateAssignments, ", ")
	}
	
	mergeSQL := fmt.Sprintf(`
		MERGE %s AS target
		USING %s AS source
		ON %s
		WHEN MATCHED THEN
		  UPDATE %s
		WHEN NOT MATCHED THEN
		  INSERT (%s) VALUES (%s)
	`, target, source, onClause, updateClause, insertFields, insertValues)
	
	return mergeSQL
}

// InsertOrUpdateBatched performs upsert operations in batches for large datasets
//
// Parameters:
//   - c: The BigQuery client instance
//   - datasetID: The ID of the dataset containing the target table
//   - tableID: The ID of the table to insert/update data
//   - rows: A slice of structs of type T representing the rows to upsert
//   - uniqueFields: Field names that should be used to match existing rows
//   - batchSize: The number of rows to process per batch (defaults to 1000 if <= 0)
//
// Returns:
//   - *QueryStats: Statistics about the operation including total rows affected
//   - error: Any errors encountered during the batched upsert operation
//
// Example Usage:
//
//	// Large dataset with 10,000 records
//	var people []Person
//	for i := 0; i < 10000; i++ {
//	    people = append(people, Person{
//	        ID: i + 1,
//	        Name: fmt.Sprintf("Person%d", i),
//	        Email: fmt.Sprintf("person%d@example.com", i),
//	        Age: 20 + i%50,
//	    })
//	}
//
//	stats, err := InsertOrUpdateBatched(client, "my_dataset", "people_table", people, []string{"id"}, 500)
//	if err != nil {
//	    log.Fatal("Batched insert or update failed:", err)
//	}
//
//	fmt.Printf("Total affected rows: %d\n", stats.RowsAffected)
func InsertOrUpdateBatched[T any](c *Client, datasetID, tableID string, rows []T, uniqueFields []string, batchSize int) (*QueryStats, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	
	totalAffected := int64(0)
	var allJobIDs []string
	
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		
		batch := rows[i:end]
		stats, err := InsertOrUpdate(c, datasetID, tableID, batch, uniqueFields)
		if err != nil {
			return nil, fmt.Errorf("batch %d-%d failed: %w", i, end-1, err)
		}
		
		totalAffected += stats.RowsAffected
		allJobIDs = append(allJobIDs, stats.JobID)
	}
	
	return &QueryStats{
		RowsAffected: totalAffected,
		JobID:        strings.Join(allJobIDs, ","),
	}, nil
}
