package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// MakeRequest sends an HTTP request with the specified method, URL, and parameters,
// then unmarshals the JSON response into the provided struct.
//
// Parameters:
//   - method: HTTP method (GET, POST, PUT, PATCH, DELETE, etc.)
//   - url: The target URL
//   - res: Pointer to struct where response will be unmarshaled
//   - body: Request body (will be JSON marshaled), pass nil for GET requests
//   - params: Query parameters as key-value pairs
//   - headers: HTTP headers as key-value pairs
//   - printRawBody: true/false to print the raw unmarshaled body
//
// Returns an error if the request fails, status code is not 2xx, or JSON unmarshaling fails.
//
// Example usage:
//
//	// GET request
//	var user User
//	err := MakeRequest("GET", "https://api.example.com/users/1", &user, nil, nil, nil, false)
//
//	// POST request with body
//	newUser := User{Name: "John", Email: "john@example.com"}
//	var createdUser User
//	err := MakeRequest("POST", "https://api.example.com/users", &createdUser, newUser, nil, nil, false)
//
//	// GET with query parameters
//	params := map[string]string{"page": "1", "limit": "10"}
//	var users []User
//	err := MakeRequest("GET", "https://api.example.com/users", &users, nil, params, nil, false)
//
//	// POST with custom headers
//	headers := map[string]string{"Authorization": "Bearer token123"}
//	err := MakeRequest("POST", "https://api.example.com/protected", &result, data, nil, headers, false)
func MakeRequest[T any](method string, url string, res *T, body any, params map[string]string, headers map[string]string, printRawBody ...bool) error {
	// create client
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// if body exist, prep it for request
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("Error Marshaling Request Body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	// init new http request to build on
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return fmt.Errorf("Error Building Request: %w", err)
	}

	// add query params
	query := req.URL.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	req.URL.RawQuery = query.Encode()

	// ensure content type is in header
	if headers == nil {
		headers = make(map[string]string)
	}
	if _, exists := headers["Content-Type"]; !exists {
		headers["Content-Type"] = "application/json"
	}

	// add headers
	for key, value := range headers {
		req.Header.Add(key, value)
	}

	// make request
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Error Making Request: %w", err)
	}
	defer response.Body.Close()

	// check status code
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP Error: %d %s", response.StatusCode, response.Status)
	}

	// read the request body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("Error Reading Response Body: %w", err)
	}

	shouldPrint := false
	if len(printRawBody) > 0 {
		shouldPrint = printRawBody[0]
	}
	if shouldPrint {
		fmt.Printf("Response Body: %v", string(responseBody))
	}

	if err = json.Unmarshal(responseBody, res); err != nil {
		return fmt.Errorf("Error Unmarshaling Response: %w", err)
	}

	return nil
}

// MakeGraphQLRequest sends a GraphQL request to the specified endpoint and
// unmarshals the response data into the provided struct.
//
// Parameters:
//   - url: GraphQL endpoint URL
//   - query: GraphQL query or mutation string
//   - variables: Variables for the GraphQL query (can be nil)
//   - res: Pointer to struct where response data will be unmarshaled
//   - headers: HTTP headers as key-value pairs (Authorization, etc.)
//
// Returns an error if the request fails, contains GraphQL errors, or JSON unmarshaling fails.
//
// Example usage:
//
//	// Simple query
//	query := `query { user(id: "1") { name email } }`
//	var user User
//	err := MakeGraphQLRequest("https://api.example.com/graphql", query, nil, &user, nil)
//
//	// Query with variables
//	query := `query GetUser($id: ID!) { user(id: $id) { name email } }`
//	variables := map[string]interface{}{"id": "123"}
//	var user User
//	err := MakeGraphQLRequest("https://api.example.com/graphql", query, variables, &user, nil)
//
//	// With authentication
//	headers := map[string]string{"Authorization": "Bearer token123"}
//	err := MakeGraphQLRequest("https://api.example.com/graphql", query, nil, &user, headers)
//
//	// Mutation
//	mutation := `mutation CreateUser($input: UserInput!) { createUser(input: $input) { id name } }`
//	variables := map[string]interface{}{"input": map[string]interface{}{"name": "John", "email": "john@example.com"}}
//	var result CreateUserResult
//	err := MakeGraphQLRequest("https://api.example.com/graphql", mutation, variables, &result, nil)
func MakeGraphQLRequest[T any](url string, query string, variables map[string]any, res *T, headers map[string]string) error {
	gqlReq := GraphQLRequest{
		Query:     query,
		Variables: variables,
	}

	var gqlRes GraphQLResponse[T]
	err := MakeRequest("POST", url, &gqlRes, gqlReq, nil, headers)
	if err != nil {
		return fmt.Errorf("GraphQL request failed: %w", err)
	}

	if len(gqlRes.Errors) > 0 {
		return fmt.Errorf("GraphQL errors: %v", gqlRes.Errors)
	}

	*res = gqlRes.Data
	return nil
}
