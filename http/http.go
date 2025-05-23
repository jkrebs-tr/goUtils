package bigcommerce

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
//
// Returns an error if the request fails, status code is not 2xx, or JSON unmarshaling fails.
//
// Example usage:
//
//	// GET request
//	var user User
//	err := MakeRequest("GET", "https://api.example.com/users/1", &user, nil, nil, nil)
//
//	// POST request with body
//	newUser := User{Name: "John", Email: "john@example.com"}
//	var createdUser User
//	err := MakeRequest("POST", "https://api.example.com/users", &createdUser, newUser, nil, nil)
//
//	// GET with query parameters
//	params := map[string]string{"page": "1", "limit": "10"}
//	var users []User
//	err := MakeRequest("GET", "https://api.example.com/users", &users, nil, params, nil)
//
//	// POST with custom headers
//	headers := map[string]string{"Authorization": "Bearer token123"}
//	err := MakeRequest("POST", "https://api.example.com/protected", &result, data, nil, headers)
func MakeRequest[T any](method string, url string, res *T, body any, params map[string]string, headers map[string]string) error {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("Error Marshaling Request Body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)

		if headers == nil {
			headers = make(map[string]string)
		}
		if _, exists := headers["Content-Type"]; !exists {
			headers["Content-Type"] = "application/json"
		}
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return fmt.Errorf("Error Building Request: %w", err)
	}

	query := req.URL.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	req.URL.RawQuery = query.Encode()

	for key, value := range headers {
		req.Header.Add(key, value)
	}

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Error Making Request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP Error: %d %s", response.StatusCode, response.Status)
	}

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("Error Reading Response Body: %w", err)
	}

	if err = json.Unmarshal(responseBody, res); err != nil {
		return fmt.Errorf("Error Unmarshaling Response: %w", err)
	}

	return nil
}
