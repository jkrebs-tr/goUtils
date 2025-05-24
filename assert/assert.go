package assert

import (
	"fmt"
	"log"
	"strings"
)

// AssertEqual checks if the actual value equals the expected value
//
// Parameters:
//   - actual: The actual value of type T to compare
//   - expected: The expected value of type T to compare against
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	AssertEqual(42, 42)        // Passes
//	AssertEqual("test", "test") // Passes
//	AssertEqual(5, 10)         // Fails and terminates program
func AssertEqual[T comparable](actual T, expected T) {
	if actual != expected {
		log.Fatalf("Assertion Failed!\nExpected Value: %v\nActual Value: %v", expected, actual)
	}
}

// AssertNotEqual checks if the actual value does not equal the expected value
//
// Parameters:
//   - actual: The actual value of type T to compare
//   - expected: The expected value of type T that should not match the actual value
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	AssertNotEqual(42, 24)     // Passes - values are different
//	AssertNotEqual("test", "demo") // Passes - strings are different
//	AssertNotEqual(5, 5)       // Fails and terminates program
func AssertNotEqual[T comparable](actual T, expected T) {
	if actual == expected {
		log.Fatalf("Assertion Failed!\nExpected Value: %v\nActual Value: %v", expected, actual)
	}
}

// AssertTrue checks if the condition is true, with optional message and arguments
//
// Parameters:
//   - cond: The boolean condition to check
//   - msgAndArgs: Optional message and arguments to include in failure output
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	AssertTrue(5 > 3)                          // Passes
//	AssertTrue(len("test") == 4)               // Passes
//	AssertTrue(false, "This should be true")   // Fails with custom message
//	AssertTrue(2 > 5, "Expected", 2, "to be greater than", 5) // Fails with formatted message
func AssertTrue(cond bool, msgAndArgs ...any) {
	if !cond {
		log.Fatalf("Assertion Failed! Expected true. %s", fmt.Sprint(msgAndArgs...))
	}
}

// AssertFalse checks if the condition is false, with optional message and arguments
//
// Parameters:
//   - cond: The boolean condition to check
//   - msgAndArgs: Optional message and arguments to include in failure output
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	AssertFalse(5 < 3)                          // Passes
//	AssertFalse(len("test") == 5)               // Passes
//	AssertFalse(true, "This should be false")   // Fails with custom message
//	AssertFalse(2 < 5, "Expected", 2, "to not be less than", 5) // Fails with formatted message
func AssertFalse(cond bool, msgAndArgs ...any) {
	if cond {
		log.Fatalf("Assertion Failed! Expected false. %s", fmt.Sprint(msgAndArgs...))
	}
}

// AssertNil checks if the object is nil (handles various nil types including pointers, slices, maps, etc.)
//
// Parameters:
//   - obj: The object to check for nil
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	var ptr *int
//	AssertNil(ptr)                    // Passes
//
//	var slice []string
//	AssertNil(slice)                  // Passes
//
//	var m map[string]int
//	AssertNil(m)                      // Passes
//
//	str := "not nil"
//	AssertNil(str)                    // Fails - string is not nillable
func AssertNil(obj any) {
	if !isNil(obj) {
		log.Fatalf("Assertion Failed! Expected nil, got: %#v", obj)
	}
}

// AssertNotNil checks if the object is not nil (handles various nil types including pointers, slices, maps, etc.)
//
// Parameters:
//   - obj: The object to check for non-nil
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	str := "not nil"
//	AssertNotNil(str)                 // Passes
//
//	slice := []string{"test"}
//	AssertNotNil(slice)               // Passes
//
//	var ptr *int
//	AssertNotNil(ptr)                 // Fails - pointer is nil
func AssertNotNil(obj any) {
	if isNil(obj) {
		log.Fatalf("Assertion Failed! Expected non-nil, got nil")
	}
}

// AssertError checks if an error is not nil, with optional message and arguments
//
// Parameters:
//   - err: The error to check
//   - msgAndArgs: Optional message and arguments to include in failure output
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	_, err := os.Open("nonexistent.txt")
//	AssertError(err)                           // Passes - file doesn't exist
//
//	_, err = os.Open("existing.txt")
//	AssertError(err, "Expected file open to fail") // Fails if file exists
func AssertError(err error, msgAndArgs ...any) {
	if err == nil {
		log.Fatalf("Assertion Failed! Expected an error. %s", fmt.Sprint(msgAndArgs...))
	}
}

// AssertNoError checks if an error is nil, with optional message and arguments
//
// Parameters:
//   - err: The error to check
//   - msgAndArgs: Optional message and arguments to include in failure output
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	file, err := os.Create("test.txt")
//	AssertNoError(err)                         // Passes if file creation succeeds
//	defer file.Close()
//
//	_, err = strconv.Atoi("not a number")
//	AssertNoError(err, "String conversion should work") // Fails with custom message
func AssertNoError(err error, msgAndArgs ...any) {
	if err != nil {
		log.Fatalf("Assertion Failed! Unexpected error: %v. %s", err, fmt.Sprint(msgAndArgs...))
	}
}

// AssertContains checks if a string contains a substring
//
// Parameters:
//   - s: The string to search in
//   - substr: The substring to search for
//
// Returns:
//   - None (calls log.Fatalf and terminates program on assertion failure)
//
// Example Usage:
//
//	AssertContains("hello world", "world")     // Passes
//	AssertContains("testing", "test")          // Passes
//	AssertContains("hello", "goodbye")         // Fails
func AssertContains(s, substr string) {
	if !strings.Contains(s, substr) {
		log.Fatalf("Assertion Failed! Expected \"%s\" to contain \"%s\"", s, substr)
	}
}
