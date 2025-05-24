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

type QueryStats struct {
    RowsAffected int64
    JobID        string
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
func (c *Client) Query(sqlQuery string, dest interface{}, params ...bigquery.QueryParameter) error {
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
