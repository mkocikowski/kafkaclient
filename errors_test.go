package kafkaclient

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/mkocikowski/libkafka"
)

func TestUnitErrorf(t *testing.T) {
	//e := WrapError(fmt.Errorf("foo: %w", &libkafka.Error{Code: 1}))
	e := Errorf("foo: %w", &libkafka.Error{Code: 1})
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if s := string(b); s != `"foo: error code 1 (OFFSET_OUT_OF_RANGE)"` {
		t.Fatal(s)
	}
}

func TestUnitErrorIs(t *testing.T) {
	bar := errors.New("bar")
	foo := Errorf("foo: %w", bar)
	if !errors.Is(foo, bar) {
		t.Fatal("is not")
	}

}
