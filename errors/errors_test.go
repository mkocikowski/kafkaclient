package errors

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestUnitError(t *testing.T) {
	base := errors.New("bar")
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  base,
			want: `{}`,
		},
		{
			err:  nil,
			want: `null`,
		},
		{
			err:  Wrap(nil),
			want: `null`,
		},
		{
			err:  Wrap(base),
			want: `"bar"`,
		},
		{
			err:  Format("foo: %w", base),
			want: `"foo: bar"`,
		},
	}
	for i, test := range tests {
		b, err := json.Marshal(test.err)
		if err != nil {
			t.Fatal(err)
		}
		if s := string(b); s != test.want {
			t.Fatal(i, test.err, s)
		}
	}
}

func TestUnitErrorIs(t *testing.T) {
	bar := errors.New("bar")
	foo := Format("foo: %w", bar)
	if !errors.Is(foo, bar) {
		t.Fatal("is not")
	}

}
