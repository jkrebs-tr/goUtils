package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Client is a wrapper around the official mongo.Client. It holds:
//   - Raw: the underlying *mongo.Client instance.
//   - DB:  a pre-selected *mongo.Database (optional).
//   - Coll: a pre-selected *mongo.Collection (optional).
type Client struct {
	Raw  *mongo.Client
	DB   *mongo.Database
	Coll *mongo.Collection
}

// NewConnection creates a new MongoDB client, verifies the connection by pinging,
// and selects the initial database and collection. It returns a configured Client
// instance with Raw set to the connected *mongo.Client, DB set to the named database,
// and Coll set to the named collection.
//
// Parameters:
//   - uri: MongoDB connection URI (e.g., "mongodb://localhost:27017").
//   - dbName: Name of the database to select (e.g., "mydb").
//   - collName: Name of the collection to select within that database (e.g., "users").
//
// Returns:
//   - *Client: A pointer to a Client wrapper containing the connected client, database, and collection.
//   - error:  Non-nil if connection, ping, or selection fails.
//
// Example usage:
//
//	// Connect to MongoDB at localhost, use "testdb" and "items" collection
//	client, err := NewConnection("mongodb://localhost:27017", "testdb", "items")
//	if err != nil {
//	    log.Fatalf("failed to connect: %v", err)
//	}
//	defer client.Close()
func NewConnection(uri, dbName, collName string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rawClient, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := rawClient.Ping(ctx, nil); err != nil {
		rawClient.Disconnect(ctx) // clean up on failure
		return nil, err
	}

	db := rawClient.Database(dbName)
	coll := db.Collection(collName)

	return &Client{
		Raw:  rawClient,
		DB:   db,
		Coll: coll,
	}, nil
}

// Close disconnects the underlying MongoDB client using a 5-second timeout context.
// After Close returns, the Client's Raw pointer should no longer be used.
//
// Returns:
//   - error: Non-nil if the disconnect operation fails.
//
// Example usage:
//
//	// Assuming 'client' is *Client returned from NewConnection
//	err := client.Close()
//	if err != nil {
//	    log.Printf("failed to close client: %v", err)
//	}
func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Raw.Disconnect(ctx)
}

// Database returns a handle to the specified database by name, using the underlying client.
// You can use this to obtain a different database than the one originally selected in NewConnection.
//
// Parameters:
//   - name: Name of the database to retrieve (e.g., "otherdb").
//
// Returns:
//   - *mongo.Database: A pointer to the requested database.
//
// Example usage:
//
//	// Get a different database named "analytics"
//	analyticsDB := client.Database("analytics")
//	// Then use analyticsDB.Collection("reports")
func (c *Client) Database(name string) *mongo.Database {
	return c.Raw.Database(name)
}

// Collection returns a handle to the specified collection within the specified database.
// It calls Database(dbName) internally and then Collection(collName) on that database.
// This is useful for retrieving a collection not stored in c.Coll.
//
// Parameters:
//   - dbName:   Name of the database to use (e.g., "usersdb").
//   - collName: Name of the collection to retrieve (e.g., "profiles").
//
// Returns:
//   - *mongo.Collection: A pointer to the requested collection.
//
// Example usage:
//
//	// Get the "orders" collection from the "ecommerce" database
//	ordersColl := client.Collection("ecommerce", "orders")
//	// Then you can call ordersColl.Find(...), etc.
func (c *Client) Collection(dbName, collName string) *mongo.Collection {
	return c.Raw.Database(dbName).Collection(collName)
}

// GetAllDocuments fetches every document from the Client’s configured collection (c.Coll).
// It creates a 5-second timeout context, executes a Find with an empty filter (bson.M{}),
// and decodes all results into a slice of bson.M maps. If you prefer to query a different
// collection, use client.Database().Collection(...) directly instead.
//
// Returns:
//   - []bson.M: A slice of documents (each document as a bson.M map).
//   - error:    Non-nil if the Find operation, cursor iteration, or decoding fails.
//
// Example usage:
//
//	// Assuming 'client' is *Client with c.Coll already set to "items" collection
//	items, err := client.GetAllDocuments()
//	if err != nil {
//	    log.Fatalf("failed to fetch documents: %v", err)
//	}
//	for _, doc := range items {
//	    fmt.Printf("%+v\n", doc)
//	}
func (c *Client) GetAllDocuments() ([]bson.M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := c.Coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}
