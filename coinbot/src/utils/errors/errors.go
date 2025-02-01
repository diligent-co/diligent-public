// Centari error tools
package errors

import (
	"fmt"
	"runtime"
)

// WrapE wraps the original error with a static error message.
// It returns a new error that includes both the static error and the original error.
func WrapE(staticErr, originalErr error) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %w: %w", file, line, staticErr, originalErr)
}

func Wrap(err error, msg string) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %w: %s", file, line, err, msg)
}

// Wrapf wraps an error with a formatted message.
// It returns a new error that includes the original error and the formatted message.
func Wrapf(err error, format string, args ...any) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %w: %s", file, line, err, fmt.Sprintf(format, args...))
}

// Wrapef wraps the original error with a static error message and a formatted message.
// It returns a new error that includes the static error, the formatted message, and the original error.
func Wrapef(staticErr, originalErr error, format string, args ...any) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %w: %s", file, line, staticErr, fmt.Sprintf(format, args...))
}

// New creates a new error with the given text.
// It's a convenience wrapper around errors.New.
func New(text string) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %s", file, line, text)
}

// Newf creates a new error with a formatted message.
// It's a convenience wrapper around fmt.Errorf.
func Newf(format string, args ...any) error {
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %s", file, line, fmt.Sprintf(format, args...))
}
