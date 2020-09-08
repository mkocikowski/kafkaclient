package errors

import (
	"errors"
	"fmt"
)

// New returns an instance of JsonError.
func New(message string) error {
	return &JsonError{error: errors.New(message)}
}

// Format is analogous to fmt.Errorf returning instance of JsonError.
func Format(format string, v ...interface{}) error {
	return &JsonError{fmt.Errorf(format, v...)}
}

// Wrap err, returning instance of JsonError. If err is nil, return nil.
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	return &JsonError{error: err}
}

// JsonError wraps error and implements MarshalJSON so that errors that are
// parts of structs are properly serialized.
type JsonError struct {
	error
}

func (e *JsonError) Unwrap() error {
	return e.error
}

func (e *JsonError) MarshalJSON() ([]byte, error) {
	return []byte(`"` + e.Error() + `"`), nil
}
