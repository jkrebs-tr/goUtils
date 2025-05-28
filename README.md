# goUtils

A collection of **Go** utility wrappers to streamline personal common tasks

---

## Table of Contents

* [Installation](#installation)
* [Packages](#packages)

  * [bigquery](#bigquery)
  * [http](#http)
  * [csv](#csv)
  * [netsuite](#netsuite)
  * [monday](#monday)
  * [rateLimiter](#ratelimiter)
  * [ses](#ses)
  * [assert](#assert)
  * [chatgpt](#chatgpt)
* [Contributing](#contributing)
* [License](#license)

---

## Installation

```bash
# Clone this repo
git clone https://github.com/yourorg/goUtils.git
cd goUtils

# Tidy dependencies
go mod tidy
```

Each package lives in its own folder under the repo root. Use them by importing:

```go
import (
    "github.com/yourorg/goUtils/bigquery"
    "github.com/yourorg/goUtils/http"
    // …etc
)
```

---

## Packages

### bigquery

Wrappers around the Google Cloud BigQuery client using generics:

* **`NewClient(ctx, projectID) (*Client, error)`** – instantiate a BQ client.
* **`StreamingInsert[T any]`** – type-safe streaming inserts (`[]T` ➔ BigQuery).
* **`StreamingInsertWithInsertIDs[T bigquery.ValueSaver]`** – de‐dup aware inserts.
* **`StreamingInsertBatched[T any]`** – batched insert helper.
* **`Query[T any]`** – run SQL and scan results into `[]T` via `Iterator.Next(&T)`.

```go
// Insert a slice of Person structs
stats, err := bigquery.StreamingInsert(client, "dataset", "table", []Person{{Name:"Alice", Age:30}})
```

---

### http

Generic REST and GraphQL clients with zero-copy JSON mapping:

* **`MakeRequest[T any](method, url string, res *T, body any, params, headers map[string]string) error`**
* **`MakeGraphQLRequest[T any](url, query string, variables map[string]any, res *T, headers map[string]string) error`**

```go
var resp MyResponse
err := http.MakeRequest("GET", "https://api.example.com", &resp, nil, nil, nil)
```

---

### csv

Helpers for CSV file generation and parsing:

* **`ReadCSV[T any](fileName string, result *[]T) ([]*T, error)`** – read into slice of `T` via struct tags.
* **`CreateFile(fileName string, headers []string) (*os.File, *csv.Writer, error)`** – init new CSV file.
* **`AppendFile(fileName string) (*os.File, *csv.Writer, error)`** – append to existing CSV.

```go
var users []User
_, err := csv.ReadCSV("users.csv", &users)
```

---

### netsuite

Simple SQL‐like wrapper for NetSuite connectors:

* **`NewConnection(connStr string) (*Connection, error)`**
* **`(*Connection) Select(query string, dest interface{}, args ...interface{}) error`** – load rows into a slice (requires manual mapping per type).

```go
var invoices []Invoice
conn, _ := netsuite.NewConnection(connStr)
_ = conn.Select("SELECT * FROM invoices", &invoices)
```

---

### monday

Minimal Monday.com API client:

* **`MakeRequest()`** – stub for future expansion.

### rateLimiter

Token‐bucket rate limiter:

* **`NewRateLimiter(rps int) *RateLimiter`**
* **`(*RateLimiter) Wait()`** – block until next token is available.

---

### ses

AWS SES email helpers:

* **`NewSESClient(region string) (*SESClient, error)`**
* **`(*SESClient) SendEmail(EmailConfig) error`**
* **`(*SESClient) SendEmailBulk(configs []EmailConfig) error`**

```go
stats, err := ses.SendEmailBulk(configs)
```

---

### assert

Assertions for quick tests and sanity checks:

* **`AssertEqual[T comparable](actual, expected T)`** – fails if `actual != expected`.
* **`AssertNotEqual[T comparable](actual, expected T)`** – fails if `actual == expected`.
* **`AssertTrue(cond bool, msgAndArgs ...any)`** – fails if `cond` is false.
* **`AssertFalse(cond bool, msgAndArgs ...any)`** – fails if `cond` is true.
* **`AssertNil(obj any)`** – fails if `obj` is not nil (handles interface-wrapped nils).
* **`AssertNotNil(obj any)`** – fails if `obj` is nil.
* **`AssertError(err error)`** – fails if `err` is nil.
* **`AssertNoError(err error)`** – fails if `err` is non-nil.
* **`AssertContains(s, substr string)`** – fails if `substr` is not found in `s`.
* **`AssertPanics(fn func())`** – fails unless `fn()` panics.

```go
import "github.com/yourorg/goUtils/assert"

func TestFoo(t *testing.T) {
    assert.AssertEqual(got, want)
    assert.AssertNoError(err)
    assert.AssertPanics(func() { Foo(nil) })
}
```

---

### chatgpt

minimal chatgpt interface to send requests to openai models

* **`SendRequest(model string, messages []Message, tmp float32, key string) (Response, error)`** – send a request to gpt`.

```go
import "github.com/yourorg/goUtils/chatgpt"

messages := []Message{
    {Role: "system", Content: "You are a helpful assistant"
    {Role: "user", Content: "Tell me a joke"
}

response, err := chatgpt.SendRequest("gpt-4", messages, 0.7, os.Getenv("OPEN_API_KEY"))

```

---

## Contributing

1. Fork this repo
2. Create a feature branch (`git checkout -b feat/xyz`)
3. Make your changes and add tests
4. Submit a PR

---

## License

MIT © Trinity Road
