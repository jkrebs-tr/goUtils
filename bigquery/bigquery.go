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

func (c *Client) Close() error {
    return c.bq.Close()
}

// StreamingInsert performs a streaming insert operation
func (c *Client) StreamingInsert(datasetID, tableID string, rows any) (*StreamingStats, error) {
    dataset := c.bq.Dataset(datasetID)
    table := dataset.Table(tableID)
    inserter := table.Inserter()
    
    // Convert rows to the format BigQuery expects
    rowsValue := reflect.ValueOf(rows)
    if rowsValue.Kind() != reflect.Slice && rowsValue.Kind() != reflect.Array {
        return nil, fmt.Errorf("rows must be a slice or array")
    }
    
    // Create a slice of any to hold the rows
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

// Query executes a BigQuery SQL and scans results into dest slice
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
