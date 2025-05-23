package csv

type parseResult[T any] struct {
	res T
	err error
	raw string
}
