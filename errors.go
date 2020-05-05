package kafkaclient

import "fmt"

func Errorf(format string, v ...interface{}) error {
	return &Error{fmt.Errorf(format, v...)}
}

/*
func WrapError(err error) error {
	if err == nil {
		return nil
	}
	return &Error{e: err}
}
*/

type Error struct {
	error
}

func (e *Error) Unwrap() error {
	return e.error
}

func (e *Error) MarshalJSON() ([]byte, error) {
	return []byte(`"` + e.Error() + `"`), nil
}
