package resp3

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	. "testing"

	"errors"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekAndAssertPrefix(t *T) {
	type test struct {
		in, prefix []byte
		exp        error
	}

	tests := []test{
		{[]byte(":5\r\n"), NumberPrefix, nil},
		{[]byte(":5\r\n"), SimpleStringPrefix, resp.ErrConnUsable{
			Err: errUnexpectedPrefix{
				Prefix: NumberPrefix, ExpectedPrefix: SimpleStringPrefix,
			},
		}},
		{[]byte("-foo\r\n"), SimpleErrorPrefix, nil},
		// TODO BlobErrorPrefix
		{[]byte("-foo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: SimpleError{
			E: errors.New("foo"),
		}}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(test.in))
			err := peekAndAssertPrefix(br, test.prefix, false)

			assert.IsType(t, test.exp, err)
			if expUsable, ok := test.exp.(resp.ErrConnUsable); ok {
				usable, _ := err.(resp.ErrConnUsable)
				assert.IsType(t, expUsable.Err, usable.Err)
			}
			if test.exp != nil {
				assert.Equal(t, test.exp.Error(), err.Error())
			}
		})
	}
}

func TestRESPTypes(t *T) {
	// TODO only used by BulkReader test
	//newLR := func(s string) resp.LenReader {
	//	buf := bytes.NewBufferString(s)
	//	return resp.NewLenReader(buf, int64(buf.Len()))
	//}

	newBigInt := func(s string) *big.Int {
		i, _ := new(big.Int).SetString(s, 10)
		return i
	}

	type encodeTest struct {
		in  resp.Marshaler
		exp string

		// unmarshal is the string to unmarshal. defaults to exp if not set.
		unmarshal string

		// if set then when exp is unmarshaled back into in the value will be
		// asserted to be this value rather than in.
		expUnmarshal interface{}

		errStr bool

		// if set then the test won't be performed a second time with a
		// preceding attribute element when unmarshaling
		noAttrTest bool
	}

	encodeTests := []encodeTest{
		{in: &BlobStringBytes{B: nil}, exp: "$0\r\n\r\n"},
		{in: &BlobStringBytes{B: []byte{}}, exp: "$0\r\n\r\n",
			expUnmarshal: &BlobStringBytes{B: nil}},
		{in: &BlobStringBytes{B: []byte("foo")}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobStringBytes{B: []byte("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobStringBytes{StreamedStringHeader: true}, exp: "$?\r\n"},
		{in: &BlobString{S: ""}, exp: "$0\r\n\r\n"},
		{in: &BlobString{S: "foo"}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobString{S: "foo\r\nbar"}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobString{StreamedStringHeader: true}, exp: "$?\r\n"},

		{in: &SimpleString{S: ""}, exp: "+\r\n"},
		{in: &SimpleString{S: "foo"}, exp: "+foo\r\n"},

		{in: &SimpleError{E: errors.New("")}, exp: "-\r\n", errStr: true},
		{in: &SimpleError{E: errors.New("foo")}, exp: "-foo\r\n", errStr: true},

		{in: &Number{N: 5}, exp: ":5\r\n"},
		{in: &Number{N: 0}, exp: ":0\r\n"},
		{in: &Number{N: -5}, exp: ":-5\r\n"},

		{in: &Null{}, exp: "_\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "$-1\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "*-1\r\n"},

		{in: &Double{F: 0}, exp: ",0\r\n"},
		{in: &Double{F: 1.5}, exp: ",1.5\r\n"},
		{in: &Double{F: -1.5}, exp: ",-1.5\r\n"},
		{in: &Double{F: math.Inf(1)}, exp: ",inf\r\n"},
		{in: &Double{F: math.Inf(-1)}, exp: ",-inf\r\n"},

		{in: &Boolean{B: false}, exp: "#f\r\n"},
		{in: &Boolean{B: true}, exp: "#t\r\n"},

		{in: &BlobError{E: errors.New("")}, exp: "!0\r\n\r\n"},
		{in: &BlobError{E: errors.New("foo")}, exp: "!3\r\nfoo\r\n"},
		{in: &BlobError{E: errors.New("foo\r\nbar")}, exp: "!8\r\nfoo\r\nbar\r\n"},

		{in: &VerbatimStringBytes{B: nil, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimStringBytes{B: []byte{}, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n",
			expUnmarshal: &VerbatimStringBytes{B: nil, Format: []byte("txt")}},
		{in: &VerbatimStringBytes{B: []byte("foo"), Format: []byte("txt")}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimStringBytes{B: []byte("foo\r\nbar"), Format: []byte("txt")}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},
		{in: &VerbatimString{S: "", Format: "txt"}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimString{S: "foo", Format: "txt"}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimString{S: "foo\r\nbar", Format: "txt"}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},

		{in: &BigNumber{I: newBigInt("3492890328409238509324850943850943825024385")}, exp: "(3492890328409238509324850943850943825024385\r\n"},
		{in: &BigNumber{I: newBigInt("0")}, exp: "(0\r\n"},
		{in: &BigNumber{I: newBigInt("-3492890328409238509324850943850943825024385")}, exp: "(-3492890328409238509324850943850943825024385\r\n"},

		// TODO what to do with BulkReader
		//{in: &BulkReader{LR: newLR("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},

		{in: &ArrayHeader{NumElems: 0}, exp: "*0\r\n"},
		{in: &ArrayHeader{NumElems: 5}, exp: "*5\r\n"},
		{in: &ArrayHeader{StreamedArrayHeader: true}, exp: "*?\r\n"},

		{in: &MapHeader{NumPairs: 0}, exp: "%0\r\n"},
		{in: &MapHeader{NumPairs: 5}, exp: "%5\r\n"},
		{in: &MapHeader{StreamedMapHeader: true}, exp: "%?\r\n"},

		{in: &SetHeader{NumElems: 0}, exp: "~0\r\n"},
		{in: &SetHeader{NumElems: 5}, exp: "~5\r\n"},
		{in: &SetHeader{StreamedSetHeader: true}, exp: "~?\r\n"},

		{in: &AttributeHeader{NumPairs: 0}, exp: "|0\r\n", noAttrTest: true},
		{in: &AttributeHeader{NumPairs: 5}, exp: "|5\r\n", noAttrTest: true},

		{in: &PushHeader{NumElems: 0}, exp: ">0\r\n"},
		{in: &PushHeader{NumElems: 5}, exp: ">5\r\n"},

		{in: &StreamedStringChunkBytes{B: nil}, exp: ";0\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte{}}, exp: ";0\r\n",
			expUnmarshal: &StreamedStringChunkBytes{B: nil}},
		{in: &StreamedStringChunkBytes{B: []byte("foo")}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte("foo\r\nbar")}, exp: ";8\r\nfoo\r\nbar\r\n"},
		{in: &StreamedStringChunk{S: ""}, exp: ";0\r\n"},
		{in: &StreamedStringChunk{S: "foo"}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunk{S: "foo\r\nbar"}, exp: ";8\r\nfoo\r\nbar\r\n"},
	}

	for i, et := range encodeTests {
		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("noAttr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := et.in.MarshalRESP(buf)
			assert.Nil(t, err)
			assert.Equal(t, et.exp, buf.String())

			if et.unmarshal != "" {
				buf.Reset()
				buf.WriteString(et.unmarshal)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err = um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}

	// do the unmarshal half of the tests again, but this time with a preceding
	// attribute which should be ignored.
	for i, et := range encodeTests {
		if et.noAttrTest {
			continue
		}

		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("attr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			buf.WriteString("|1\r\n+foo\r\n+bar\r\n")
			if et.unmarshal != "" {
				buf.WriteString(et.unmarshal)
			} else {
				buf.WriteString(et.exp)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err := um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}
}

// structs used for tests
type testStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int
}

func intPtr(i int) *int {
	return &i
}

type testStructA struct {
	testStructInner
	Biz []byte
}

type testStructB struct {
	*testStructInner
	Biz []byte
}

type testStructC struct {
	Biz *string
}

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

func TestAnyMarshal(t *T) {
	type encodeTest struct {
		in               interface{}
		out              string
		forceStr, flat   bool
		expErr           bool
		expErrConnUsable bool
	}

	var encodeTests = []encodeTest{
		// Bulk strings
		{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
		{in: "ohey", out: "$4\r\nohey\r\n"},
		{in: "", out: "$0\r\n\r\n"},
		{in: true, out: "$1\r\n1\r\n"},
		{in: false, out: "$1\r\n0\r\n"},
		{in: nil, out: "$-1\r\n"},
		{in: nil, forceStr: true, out: "$0\r\n\r\n"},
		{in: []byte(nil), out: "$-1\r\n"},
		{in: []byte(nil), forceStr: true, out: "$0\r\n\r\n"},
		{in: float32(5.5), out: "$3\r\n5.5\r\n"},
		{in: float64(5.5), out: "$3\r\n5.5\r\n"},
		{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: "ohey", flat: true, out: "$4\r\nohey\r\n"},

		// Number
		{in: 5, out: ":5\r\n"},
		{in: int64(5), out: ":5\r\n"},
		{in: uint64(5), out: ":5\r\n"},
		{in: int64(5), forceStr: true, out: "$1\r\n5\r\n"},
		{in: uint64(5), forceStr: true, out: "$1\r\n5\r\n"},

		// Error
		{in: errors.New(":("), out: "-:(\r\n"},
		{in: errors.New(":("), forceStr: true, out: "$2\r\n:(\r\n"},

		// Simple arrays
		{in: []string(nil), out: "*-1\r\n"},
		{in: []string(nil), flat: true, out: ""},
		{in: []string{}, out: "*0\r\n"},
		{in: []string{}, flat: true, out: ""},
		{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},
		{in: []int{1, 2}, flat: true, out: ":1\r\n:2\r\n"},
		{in: []int{1, 2}, forceStr: true, out: "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"},
		{in: []int{1, 2}, flat: true, forceStr: true, out: "$1\r\n1\r\n$1\r\n2\r\n"},

		// Complex arrays
		{in: []interface{}{}, out: "*0\r\n"},
		{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			out:      "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			flat:     true,
			out:      "$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:     []interface{}{func() {}},
			expErr: true,
		},
		{
			in:               []interface{}{func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},
		{
			in:     []interface{}{"a", func() {}},
			flat:   true,
			expErr: true,
		},

		// Embedded arrays
		{
			in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
			out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
		},
		{
			in:   []interface{}{[]string{"a", "b"}, []int{1, 2}},
			flat: true,
			out:  "$1\r\na\r\n$1\r\nb\r\n:1\r\n:2\r\n",
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			flat:   true,
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{func() {}, "a"},
				[]interface{}{"b", func() {}},
			},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Maps
		{in: map[string]int(nil), out: "*-1\r\n"},
		{in: map[string]int(nil), flat: true, out: ""},
		{in: map[string]int{}, out: "*0\r\n"},
		{in: map[string]int{}, flat: true, out: ""},
		{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
		{
			in:  map[string]interface{}{"one": []byte("1")},
			out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
		},
		{
			in:  map[string]interface{}{"one": []string{"1", "2"}},
			out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:   map[string]interface{}{"one": []string{"1", "2"}},
			flat: true,
			out:  "$3\r\none\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			expErr: true,
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			flat:   true,
			expErr: true,
		},
		{
			in:     map[complex128]interface{}{0: func() {}},
			expErr: true,
		},
		{
			in:               map[complex128]interface{}{0: func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Structs
		{
			in: testStructA{
				testStructInner: testStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in: testStructB{
				testStructInner: &testStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructB{Biz: []byte("10")},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructC{},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$0\r\n\r\n",
		},
	}

	for i, et := range encodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			a := Any{
				I:                     et.in,
				MarshalBlobString:     et.forceStr,
				MarshalNoArrayHeaders: et.flat,
			}

			err := a.MarshalRESP(buf)
			var errConnUsable resp.ErrConnUsable
			if et.expErr && err == nil {
				t.Fatal("expected error")
			} else if et.expErr && et.expErrConnUsable != errors.As(err, &errConnUsable) {
				t.Fatalf("expected ErrConnUsable:%v, got: %v", et.expErrConnUsable, err)
			} else if !et.expErr {
				assert.Nil(t, err)
			}

			if !et.expErr {
				assert.Equal(t, et.out, buf.String(), "et: %#v", et)
			}
		})
	}
}

type textCPUnmarshaler []byte

func (cu *textCPUnmarshaler) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCPUnmarshaler []byte

func (cu *binCPUnmarshaler) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type lowerCaseUnmarshaler string

func (lcu *lowerCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*lcu = lowerCaseUnmarshaler(strings.ToLower(bs.S))
	return nil
}

type upperCaseUnmarshaler string

func (ucu *upperCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*ucu = upperCaseUnmarshaler(strings.ToUpper(bs.S))
	return nil
}

type writer []byte

func (w *writer) Write(b []byte) (int, error) {
	*w = append(*w, b...)
	return len(b), nil
}

func TestAnyUnmarshal(t *T) {
	type decodeTest struct {
		in  string
		out interface{}

		// Instead of unmarshalling into a zero-value of out's type, unmarshal
		// into a copy of this, then compare with out
		preload interface{}

		// like preload, but explicitly preload with a pointer to an empty
		// interface
		preloadEmpty bool

		// instead of testing out, test that unmarshal returns an error
		shouldErr string
	}

	decodeTests := []decodeTest{
		// Bulk string
		{in: "$-1\r\n", out: []byte(nil)},
		{in: "$-1\r\n", preload: []byte{1}, out: []byte(nil)},
		{in: "$-1\r\n", preloadEmpty: true, out: []byte(nil)},
		{in: "$0\r\n\r\n", out: ""},
		{in: "$0\r\n\r\n", out: []byte(nil)},
		{in: "$4\r\nohey\r\n", out: "ohey"},
		{in: "$4\r\nohey\r\n", out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", preload: []byte(nil), out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", preload: []byte(""), out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", preload: []byte("wut"), out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", preload: []byte("wutwut"), out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", out: textCPUnmarshaler("ohey")},
		{in: "$4\r\nohey\r\n", out: binCPUnmarshaler("ohey")},
		{in: "$4\r\nohey\r\n", out: writer("ohey")},
		{in: "$2\r\n10\r\n", out: int(10)},
		{in: "$2\r\n10\r\n", out: uint(10)},
		{in: "$4\r\n10.5\r\n", out: float32(10.5)},
		{in: "$4\r\n10.5\r\n", out: float64(10.5)},
		{in: "$4\r\nohey\r\n", preloadEmpty: true, out: []byte("ohey")},
		{in: "$4\r\nohey\r\n", out: nil},

		// Simple string
		{in: "+\r\n", out: ""},
		{in: "+\r\n", out: []byte(nil)},
		{in: "+ohey\r\n", out: "ohey"},
		{in: "+ohey\r\n", out: []byte("ohey")},
		{in: "+ohey\r\n", out: textCPUnmarshaler("ohey")},
		{in: "+ohey\r\n", out: binCPUnmarshaler("ohey")},
		{in: "+ohey\r\n", out: writer("ohey")},
		{in: "+10\r\n", out: int(10)},
		{in: "+10\r\n", out: uint(10)},
		{in: "+10.5\r\n", out: float32(10.5)},
		{in: "+10.5\r\n", out: float64(10.5)},
		{in: "+ohey\r\n", preloadEmpty: true, out: "ohey"},
		{in: "+ohey\r\n", out: nil},

		// Err
		{in: "-ohey\r\n", out: "", shouldErr: "ohey"},
		{in: "-ohey\r\n", out: nil, shouldErr: "ohey"},

		// Number
		{in: ":1024\r\n", out: "1024"},
		{in: ":1024\r\n", out: []byte("1024")},
		{in: ":1024\r\n", out: textCPUnmarshaler("1024")},
		{in: ":1024\r\n", out: binCPUnmarshaler("1024")},
		{in: ":1024\r\n", out: writer("1024")},
		{in: ":1024\r\n", out: int(1024)},
		{in: ":1024\r\n", out: uint(1024)},
		{in: ":1024\r\n", out: float32(1024)},
		{in: ":1024\r\n", out: float64(1024)},
		{in: ":1024\r\n", preloadEmpty: true, out: int64(1024)},
		{in: ":1024\r\n", out: nil},

		// Arrays
		{in: "*-1\r\n", out: []interface{}(nil)},
		{in: "*-1\r\n", out: []string(nil)},
		{in: "*-1\r\n", out: map[string]string(nil)},
		{in: "*-1\r\n", preloadEmpty: true, out: []interface{}(nil)},
		{in: "*0\r\n", out: []interface{}{}},
		{in: "*0\r\n", out: []string{}},
		{in: "*0\r\n", preload: map[string]string(nil), out: map[string]string{}},
		{in: "*2\r\n+foo\r\n+bar\r\n", out: []string{"foo", "bar"}},
		{in: "*2\r\n+foo\r\n+bar\r\n", out: []interface{}{"foo", "bar"}},
		{in: "*2\r\n+foo\r\n+bar\r\n", preloadEmpty: true, out: []interface{}{"foo", "bar"}},
		{in: "*2\r\n+foo\r\n+5\r\n", preload: []interface{}{0, 1}, out: []interface{}{"foo", "5"}},
		{
			in: "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
			out: []interface{}{
				[]interface{}{"foo", "bar"},
				[]interface{}{"baz"},
			},
		},
		{
			in:  "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
			out: [][]string{{"foo", "bar"}, {"baz"}},
		},
		{
			in:  "*2\r\n*2\r\n+foo\r\n+bar\r\n+baz\r\n",
			out: []interface{}{[]interface{}{"foo", "bar"}, "baz"},
		},
		{in: "*2\r\n:1\r\n:2\r\n", out: map[string]string{"1": "2"}},
		{in: "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n", out: nil},
		{
			in: "*6\r\n" +
				"$3\r\none\r\n" + "*2\r\n$1\r\n!\r\n$1\r\n1\r\n" +
				"$3\r\ntwo\r\n" + "*2\r\n$2\r\n!!\r\n$1\r\n2\r\n" +
				"$5\r\nthree\r\n" + "*2\r\n$3\r\n!!!\r\n$1\r\n3\r\n",
			out: map[string]map[string]int{
				"one":   {"!": 1},
				"two":   {"!!": 2},
				"three": {"!!!": 3},
			},
		},
		{
			in: "*4\r\n" +
				"$5\r\nhElLo\r\n" + "$5\r\nWoRlD\r\n" +
				"$3\r\nFoO\r\n" + "$3\r\nbAr\r\n",
			out: map[upperCaseUnmarshaler]lowerCaseUnmarshaler{
				"HELLO": "world",
				"FOO":   "bar",
			},
		},

		// Arrays (structs)
		{
			in: "*10\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBoz\r\n" + ":100\r\n" +
				"$3\r\nDNE\r\n" + ":1000\r\n" +
				"$3\r\nBiz\r\n" + "$1\r\n5\r\n",
			out: testStructA{
				testStructInner: testStructInner{
					Foo: 1,
					Baz: "3",
					Boz: intPtr(100),
				},
				Biz: []byte("5"),
			},
		},
		{
			in: "*10\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBiz\r\n" + "$1\r\n5\r\n" +
				"$3\r\nBoz\r\n" + ":100\r\n" +
				"$3\r\nDNE\r\n" + ":1000\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n",
			preload: testStructB{testStructInner: new(testStructInner)},
			out: testStructB{
				testStructInner: &testStructInner{
					Foo: 1,
					Baz: "3",
					Boz: intPtr(100),
				},
				Biz: []byte("5"),
			},
		},
	}

	for i, dt := range decodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			br := bufio.NewReader(bytes.NewBufferString(dt.in))

			var into interface{}
			if dt.preloadEmpty {
				emptyInterfaceT := reflect.TypeOf([]interface{}(nil)).Elem()
				into = reflect.New(emptyInterfaceT).Interface()
			} else if dt.preload != nil {
				intov := reflect.New(reflect.TypeOf(dt.preload))
				intov.Elem().Set(reflect.ValueOf(dt.preload))
				into = intov.Interface()
			} else if dt.out != nil {
				into = reflect.New(reflect.TypeOf(dt.out)).Interface()
			}

			err := Any{I: into}.UnmarshalRESP(br)
			if dt.shouldErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, dt.shouldErr, err.Error())
				return
			}

			require.Nil(t, err)
			if dt.out != nil {
				aI := reflect.ValueOf(into).Elem().Interface()
				assert.Equal(t, dt.out, aI)
			} else {
				assert.Nil(t, into)
			}
		})
	}
}

func TestRawMessage(t *T) {
	rmtests := []struct {
		b       string
		isNil   bool
		isEmpty bool
	}{
		{b: "+\r\n"},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$-1\r\n", isNil: true},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*-1\r\n", isNil: true},
		{b: "*0\r\n", isEmpty: true},
	}

	// one at a time
	for _, rmt := range rmtests {
		buf := new(bytes.Buffer)
		{
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(buf))
			assert.Equal(t, rmt.b, buf.String())
			assert.Equal(t, rmt.isNil, rm.IsNil())
			assert.Equal(t, rmt.isEmpty, rm.IsEmptyArray())
		}
		{
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(bufio.NewReader(buf)))
			assert.Equal(t, rmt.b, string(rm))
		}
	}
}

func TestAnyConsumedOnErr(t *T) {
	type foo struct {
		Foo int
		Bar int
	}

	type test struct {
		in   resp.Marshaler
		into interface{}
	}

	type unknownType string

	tests := []test{
		{Any{I: errors.New("foo")}, new(unknownType)},
		{BlobString{S: "blobStr"}, new(unknownType)},
		{SimpleString{S: "blobStr"}, new(unknownType)},
		{Number{N: 1}, new(unknownType)},
		{Any{I: []string{"one", "2", "three"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "3", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four", "five"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "three", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "3", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []interface{}{1, 2, "Bar", "two"}}, new(foo)},
		{Any{I: []string{"Foo", "1", "Bar", "two"}}, new(foo)},
		{Any{I: [][]string{{"one", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"3", "four"}}}, new([][]int)},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			require.Nil(t, test.in.MarshalRESP(buf))
			require.Nil(t, SimpleString{S: "DISCARDED"}.MarshalRESP(buf))
			br := bufio.NewReader(buf)

			err := Any{I: test.into}.UnmarshalRESP(br)
			assert.Error(t, err)
			assert.True(t, errors.As(err, new(resp.ErrConnUsable)))

			var ss SimpleString
			assert.NoError(t, ss.UnmarshalRESP(br))
			assert.Equal(t, "DISCARDED", ss.S)
		})
	}
}

func Example_streamedAggregatedType() {
	buf := new(bytes.Buffer)

	// First write a streamed array to the buffer. The array will have 3 number
	// elements.
	(ArrayHeader{StreamedArrayHeader: true}).MarshalRESP(buf)
	(Number{N: 1}).MarshalRESP(buf)
	(Number{N: 2}).MarshalRESP(buf)
	(Number{N: 3}).MarshalRESP(buf)
	(StreamedAggregatedEnd{}).MarshalRESP(buf)

	// Now create a reader which will read from the buffer, and use it to read
	// the streamed array.
	br := bufio.NewReader(buf)
	var head ArrayHeader
	head.UnmarshalRESP(br)
	if !head.StreamedArrayHeader {
		panic("expected streamed array header")
	}
	fmt.Println("streamed array begun")

	for {
		var el Number
		aggEl := StreamedAggregatedElement{Unmarshaler: &el}
		aggEl.UnmarshalRESP(br)
		if aggEl.End {
			fmt.Println("streamed array ended")
			return
		}
		fmt.Printf("read element with value %d\n", el.N)
	}

	// Output: streamed array begun
	// Output: read element with value 1
	// Output: read element with value 2
	// Output: read element with value 3
	// Output: streamed array ended
}
