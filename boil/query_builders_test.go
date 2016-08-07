package boil

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

var writeGoldenFiles = flag.Bool(
	"test.golden",
	false,
	"Write golden files.",
)

func TestBuildQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		q    *Query
		args []interface{}
	}{
		{&Query{from: []string{"t"}}, nil},
		{&Query{from: []string{"q"}, limit: 5, offset: 6}, nil},
		{&Query{from: []string{"q"}, orderBy: []string{"a ASC", "b DESC"}}, nil},
		{&Query{from: []string{"t"}, selectCols: []string{"count(*) as ab, thing as bd", `"stuff"`}}, nil},
		{&Query{from: []string{"a", "b"}, selectCols: []string{"count(*) as ab, thing as bd", `"stuff"`}}, nil},
		{&Query{
			selectCols: []string{"a.happy", "r.fun", "q"},
			from:       []string{"happiness as a"},
			joins:      []join{{clause: "rainbows r on a.id = r.happy_id"}},
		}, nil},
	}

	for i, test := range tests {
		filename := filepath.Join("_fixtures", fmt.Sprintf("%02d.sql", i))
		out, args := buildQuery(test.q)

		if *writeGoldenFiles {
			err := ioutil.WriteFile(filename, []byte(out), 0664)
			if err != nil {
				t.Fatalf("Failed to write golden file %s: %s\n", filename, err)
			}
			t.Logf("wrote golden file: %s\n", filename)
			continue
		}

		byt, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatalf("Failed to read golden file %q: %v", filename, err)
		}

		if string(bytes.TrimSpace(byt)) != out {
			t.Errorf("[%02d] Test failed:\nWant:\n%s\nGot:\n%s", i, byt, out)
		}

		if !reflect.DeepEqual(args, test.args) {
			t.Errorf("[%02d] Test failed:\nWant:\n%s\nGot:\n%s", i, spew.Sdump(test.args), spew.Sdump(args))
		}
	}
}

func TestIdentifierMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		In  Query
		Out map[string]string
	}{
		{
			In:  Query{from: []string{`a`}},
			Out: map[string]string{"a": "a"},
		},
		{
			In:  Query{from: []string{`"a"`, `b`}},
			Out: map[string]string{"a": "a", "b": "b"},
		},
		{
			In:  Query{from: []string{`a as b`}},
			Out: map[string]string{"b": "a"},
		},
		{
			In:  Query{from: []string{`a AS "b"`, `"c" as d`}},
			Out: map[string]string{"b": "a", "d": "c"},
		},
		{
			In:  Query{joins: []join{{kind: JoinInner, clause: `a on stuff = there`}}},
			Out: map[string]string{"a": "a"},
		},
		{
			In:  Query{joins: []join{{kind: JoinNatural, clause: `"a" on stuff = there`}}},
			Out: map[string]string{"a": "a"},
		},
		{
			In:  Query{joins: []join{{kind: JoinNatural, clause: `a as b on stuff = there`}}},
			Out: map[string]string{"b": "a"},
		},
		{
			In:  Query{joins: []join{{kind: JoinOuterRight, clause: `"a" as "b" on stuff = there`}}},
			Out: map[string]string{"b": "a"},
		},
	}

	for i, test := range tests {
		m := identifierMapping(&test.In)

		for k, v := range test.Out {
			val, ok := m[k]
			if !ok {
				t.Errorf("%d) want: %s = %s, but was missing", i, k, v)
			}
			if val != v {
				t.Errorf("%d) want: %s = %s, got: %s", i, k, v, val)
			}
		}
	}
}