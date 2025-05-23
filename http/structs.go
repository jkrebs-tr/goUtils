package http

type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type GraphQLResponse[T any] struct {
	Data   T                        `json:"data"`
	Errors []GraphQLError           `json:"errors,omitempty"`
}

type GraphQLError struct {
	Message   string                 `json:"message"`
	Path      []any          `json:"path,omitempty"`
	Locations []GraphQLErrorLocation `json:"locations,omitempty"`
}

type GraphQLErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}
