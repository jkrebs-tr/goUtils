package bigquery

import (
	"context"
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
		end := min(i + batchSize, len(rows))

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
