package resp3

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"testing"
	. "testing"

	"errors"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekAndAssertPrefix(t *T) {
	type test struct {
		in     []byte
		prefix Prefix
		exp    error
	}

	tests := []test{
		{[]byte(":5\r\n"), NumberPrefix, nil},
		{[]byte(":5\r\n"), SimpleStringPrefix, resp.ErrConnUsable{
			Err: errUnexpectedPrefix{
				Prefix: NumberPrefix, ExpectedPrefix: SimpleStringPrefix,
			},
		}},
		{[]byte("-foo\r\n"), SimpleErrorPrefix, nil},
		{[]byte("-foo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: SimpleError{
			S: "foo",
		}}},
		{[]byte("!3\r\nfoo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: BlobError{
			B: []byte("foo"),
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

// structs used for tests
type TestStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int64
}

func intPtr(i int) *int {
	return &i
}

type testStructA struct {
	TestStructInner
	Biz []byte
}

type testStructB struct {
	*TestStructInner
	Biz []byte
}

type testStructC struct {
	Biz *string
}

type textCP []byte

func (cu textCP) MarshalText() ([]byte, error) {
	return []byte(cu), nil
}

func (cu *textCP) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCP []byte

func (cu binCP) MarshalBinary() ([]byte, error) {
	return []byte(cu), nil
}

func (cu *binCP) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type streamedStrRW [][]byte

func (rw *streamedStrRW) Write(b []byte) (int, error) {
	rwB := append([]byte(nil), b...)
	*rw = append(*rw, rwB)
	return len(rwB), nil
}

func (rw streamedStrRW) Read(b []byte) (int, error) {
	// this method jumps through a lot of hoops to not have a pointer receiver,
	// because that makes formulating the tests easier.
	if len(rw) == 0 || len(rw[0]) == 0 {
		return 0, io.EOF
	}

	rwB := rw[0]
	if len(b) < len(rwB) {
		panic("length of byte slice being read into is smaller than what needs to be written")
	}
	copy(b, rwB)
	copy(rw, rw[1:])
	rw[len(rw)-1] = []byte{}
	return len(rwB), nil
}

type msgSeries []interface {
	resp.Marshaler
	resp.Unmarshaler
}

func (s msgSeries) MarshalRESP(w io.Writer) error {
	for i := range s {
		if err := s[i].MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

func (s msgSeries) UnmarshalRESP(br *bufio.Reader) error {
	for i := range s {
		if err := s[i].UnmarshalRESP(br); err != nil {
			return err
		}
	}
	return nil
}

type ie [2]interface{} // into/expect

type kase struct { // "case" is reserved
	label string

	// for each kase the input message will be unmarshaled into the first
	// interface{} (the "into") and then compared with the second (the
	// "expected").
	ie ie

	// reverseable, also test AnyMarshaler
	r bool

	// flatStrEmpty indicates that when marshaling as a flat string (see
	// msgAsFlatStr) the result is expected to be empty. This is only useful in
	// some very specific cases, notably nil slices/maps/structs.
	flatStrEmpty bool
}

type in struct {
	// label is optional and is used to sometimes make generating cases easier
	label string

	// msg is the actual RESP message being unmarshaled
	msg string

	// msgAsFlatStr is the msg having been flattened and all non-agg elements
	// turned into blob strings.
	msgAsFlatStr string

	// mkCases generates the test cases specific to this particular input.
	mkCases func() []kase
}

func (in in) expNumElems() int {
	var numElems int
	buf := bytes.NewBufferString(in.msgAsFlatStr)
	br := bufio.NewReader(buf)
	for {
		if br.Buffered() == 0 && buf.Len() == 0 {
			return numElems
		} else if err := (Any{}).UnmarshalRESP(br); err != nil {
			panic(err)
		}
		numElems++
	}
}

func (in in) prefix() Prefix {
	return Prefix(in.msg[0])
}

func (in in) streamed() bool {
	return in.msg[1] == '?'
}

type unmarshalMarshalTest struct {
	descr string
	ins   []in

	// mkCases generates a set of cases in a generic way for each in (useful
	// for cases which are identical or only slightly different across all
	// ins).
	mkCases func(in) []kase

	// instead of testing cases, assert that unmarshal returns this specific
	// error.
	shouldErr error
}

func (umt unmarshalMarshalTest) cases(in in) []kase {
	var cases []kase
	if in.mkCases != nil {
		cases = append(cases, in.mkCases()...)
	}
	if umt.mkCases != nil {
		cases = append(cases, umt.mkCases(in)...)
	}
	for i := range cases {
		if cases[i].label == "" {
			cases[i].label = fmt.Sprintf("case%d", i)
		}
	}
	return cases
}

func TestAnyUnmarshalMarshal(t *T) {

	strPtr := func(s string) *string { return &s }
	bytPtr := func(b []byte) *[]byte { return &b }
	intPtr := func(i int64) *int64 { return &i }
	fltPtr := func(f float64) *float64 { return &f }

	setReversable := func(to bool, kk []kase) []kase {
		for i := range kk {
			kk[i].r = to
		}
		return kk
	}

	strCases := func(in in, str string) []kase {
		prefix := in.prefix()
		kases := setReversable(prefix == BlobStringPrefix, []kase{
			{ie: ie{"", str}},
			{ie: ie{"otherstring", str}},
			{ie: ie{(*string)(nil), strPtr(str)}},
			{ie: ie{strPtr(""), strPtr(str)}},
			{ie: ie{strPtr("otherstring"), strPtr(str)}},
			{ie: ie{[]byte{}, []byte(str)}},
			{ie: ie{[]byte(nil), []byte(str)}},
			{ie: ie{[]byte("f"), []byte(str)}},
			{ie: ie{[]byte("biglongstringblaaaaah"), []byte(str)}},
			{ie: ie{(*[]byte)(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("f")), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("biglongstringblaaaaah")), bytPtr([]byte(str))}},
			{ie: ie{textCP{}, textCP(str)}},
			{ie: ie{binCP{}, binCP(str)}},
			{ie: ie{new(bytes.Buffer), bytes.NewBufferString(str)}},
		})
		if prefix == BlobStringPrefix && !in.streamed() {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{BlobString{S: ""}, BlobString{S: str}}},
				{ie: ie{BlobString{S: "f"}, BlobString{S: str}}},
				{ie: ie{BlobString{S: "biglongstringblaaaaah"}, BlobString{S: str}}},
				{ie: ie{BlobStringBytes{B: nil}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte{}}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte("f")}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte("biglongstringblaaaaah")}, BlobStringBytes{B: []byte(str)}}},
			})...)
		} else if prefix == SimpleStringPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{SimpleString{S: ""}, SimpleString{S: str}}},
				{ie: ie{SimpleString{S: "f"}, SimpleString{S: str}}},
				{ie: ie{SimpleString{S: "biglongstringblaaaaah"}, SimpleString{S: str}}},
			})...)
		}
		return kases
	}

	floatCases := func(in in, f float64) []kase {
		prefix := in.prefix()
		kases := setReversable(prefix == DoublePrefix, []kase{
			{ie: ie{float32(0), float32(f)}},
			{ie: ie{float32(1), float32(f)}},
			{ie: ie{float64(0), float64(f)}},
			{ie: ie{float64(1), float64(f)}},
			{ie: ie{(*float64)(nil), fltPtr(f)}},
			{ie: ie{fltPtr(0), fltPtr(f)}},
			{ie: ie{fltPtr(1), fltPtr(f)}},
			{ie: ie{new(big.Float), new(big.Float).SetFloat64(f)}},
		})
		kases = append(kases, []kase{
			{r: prefix == BooleanPrefix, ie: ie{false, f != 0}},
		}...)
		if prefix == DoublePrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Double{F: 0}, Double{F: f}}},
				{ie: ie{Double{F: 1.5}, Double{F: f}}},
				{ie: ie{Double{F: -1.5}, Double{F: f}}},
				{ie: ie{Double{F: math.Inf(1)}, Double{F: f}}},
				{ie: ie{Double{F: math.Inf(-1)}, Double{F: f}}},
			})...)

		} else if prefix == BooleanPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Boolean{B: false}, Boolean{B: f != 0}}},
				{ie: ie{Boolean{B: true}, Boolean{B: f != 0}}},
			})...)
		}
		return kases
	}

	intCases := func(in in, i int64) []kase {
		prefix := in.prefix()
		kases := floatCases(in, float64(i))
		kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
			{ie: ie{int(0), int(i)}},
			{ie: ie{int8(0), int8(i)}},
			{ie: ie{int16(0), int16(i)}},
			{ie: ie{int32(0), int32(i)}},
			{ie: ie{int64(0), int64(i)}},
			{ie: ie{int(1), int(i)}},
			{ie: ie{int8(1), int8(i)}},
			{ie: ie{int16(1), int16(i)}},
			{ie: ie{int32(1), int32(i)}},
			{ie: ie{int64(1), int64(i)}},
			{ie: ie{(*int64)(nil), intPtr(i)}},
			{ie: ie{intPtr(0), intPtr(i)}},
			{ie: ie{intPtr(1), intPtr(i)}},
		})...)

		kases = append(kases, []kase{
			{
				ie: ie{new(big.Int), new(big.Int).SetInt64(i)},
				r:  prefix == BigNumberPrefix,
			},
		}...)

		if i >= 0 {
			kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
				{ie: ie{uint(0), uint(i)}},
				{ie: ie{uint8(0), uint8(i)}},
				{ie: ie{uint16(0), uint16(i)}},
				{ie: ie{uint32(0), uint32(i)}},
				{ie: ie{uint64(0), uint64(i)}},
				{ie: ie{uint(1), uint(i)}},
				{ie: ie{uint8(1), uint8(i)}},
				{ie: ie{uint16(1), uint16(i)}},
				{ie: ie{uint32(1), uint32(i)}},
				{ie: ie{uint64(1), uint64(i)}},
			})...)
		}

		if prefix == NumberPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Number{N: 5}, Number{N: i}}},
				{ie: ie{Number{N: 0}, Number{N: i}}},
				{ie: ie{Number{N: -5}, Number{N: i}}},
			})...)

		} else if prefix == BigNumberPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{BigNumber{I: new(big.Int)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
				{ie: ie{BigNumber{I: new(big.Int).SetInt64(1)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
				{ie: ie{BigNumber{I: new(big.Int).SetInt64(-1)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
			})...)
		}

		return kases
	}

	nullCases := func(in in) []kase {
		cases := []kase{
			{ie: ie{[]byte(nil), []byte(nil)}},
			{ie: ie{[]byte{}, []byte(nil)}},
			{ie: ie{[]byte{1}, []byte(nil)}},
		}
		cases = append(cases, setReversable(in.prefix() == NullPrefix, []kase{
			{flatStrEmpty: true, ie: ie{[]string(nil), []string(nil)}},
			{flatStrEmpty: true, ie: ie{[]string{}, []string(nil)}},
			{flatStrEmpty: true, ie: ie{[]string{"ohey"}, []string(nil)}},
			{flatStrEmpty: true, ie: ie{map[string]string(nil), map[string]string(nil)}},
			{flatStrEmpty: true, ie: ie{map[string]string{}, map[string]string(nil)}},
			{flatStrEmpty: true, ie: ie{map[string]string{"a": "b"}, map[string]string(nil)}},
			{ie: ie{(*int64)(nil), (*int64)(nil)}},
			{ie: ie{intPtr(0), (*int64)(nil)}},
			{ie: ie{intPtr(1), (*int64)(nil)}},
			{flatStrEmpty: true, ie: ie{(*testStructA)(nil), (*testStructA)(nil)}},
			{flatStrEmpty: true, ie: ie{&testStructA{}, (*testStructA)(nil)}},
			{ie: ie{Null{}, Null{}}},
		})...)
		return cases
	}

	unmarshalMarshalTests := []unmarshalMarshalTest{
		{
			descr: "empty blob string",
			ins: []in{{
				msg:          "$0\r\n\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte{}}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "") },
		},
		{
			descr: "blob string",
			ins: []in{{
				msg:          "$4\r\nohey\r\n",
				msgAsFlatStr: "$4\r\nohey\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ohey")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ohey") },
		},
		{
			descr: "integer blob string",
			ins: []in{{
				msg:          "$2\r\n10\r\n",
				msgAsFlatStr: "$2\r\n10\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("10")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "float blob string",
			ins: []in{{
				msg:          "$4\r\n10.5\r\n",
				msgAsFlatStr: "$4\r\n10.5\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("10.5")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			// only for backwards compatibility
			descr: "null blob string",
			ins: []in{{
				msg:          "$-1\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte(nil)}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "blob string with delim",
			ins: []in{{
				msg:          "$6\r\nab\r\ncd\r\n",
				msgAsFlatStr: "$6\r\nab\r\ncd\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ab\r\ncd")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ab\r\ncd") },
		},
		{
			descr: "empty simple string",
			ins: []in{{
				msg:          "+\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, ""}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "") },
		},
		{
			descr: "simple string",
			ins: []in{{
				msg:          "+ohey\r\n",
				msgAsFlatStr: "$4\r\nohey\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "ohey"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ohey") },
		},
		{
			descr: "integer simple string",
			ins: []in{{
				msg:          "+10\r\n",
				msgAsFlatStr: "$2\r\n10\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "10"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "float simple string",
			ins: []in{{
				msg:          "+10.5\r\n",
				msgAsFlatStr: "$4\r\n10.5\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "10.5"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			descr: "empty simple error",
			ins: []in{{
				msg:          "-\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
			}},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: ""}},
		},
		{
			descr: "simple error",
			ins: []in{{
				msg:          "-ohey\r\n",
				msgAsFlatStr: "$4\r\nohey\r\n",
			}},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: "ohey"}},
		},
		{
			descr: "zero number",
			ins: []in{{
				msg:          ":0\r\n",
				msgAsFlatStr: "$1\r\n0\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(0)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "positive number",
			ins: []in{{
				msg:          ":10\r\n",
				msgAsFlatStr: "$2\r\n10\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(10)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "negative number",
			ins: []in{{
				msg:          ":-10\r\n",
				msgAsFlatStr: "$3\r\n-10\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(-10)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-10"), intCases(in, -10)...) },
		},
		{
			descr: "null",
			ins: []in{{
				msg:          "_\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, nil}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "zero double",
			ins: []in{{
				msg:          ",0\r\n",
				msgAsFlatStr: "$1\r\n0\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(0)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), floatCases(in, 0)...) },
		},
		{
			descr: "positive double",
			ins: []in{{
				msg:          ",10.5\r\n",
				msgAsFlatStr: "$4\r\n10.5\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(10.5)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			descr: "positive double infinity",
			ins: []in{{
				msg:          ",inf\r\n",
				msgAsFlatStr: "$3\r\ninf\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, math.Inf(1)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "inf"), floatCases(in, math.Inf(1))...) },
		},
		{
			descr: "negative double",
			ins: []in{{
				msg:          ",-10.5\r\n",
				msgAsFlatStr: "$5\r\n-10.5\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(-10.5)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-10.5"), floatCases(in, -10.5)...) },
		},
		{
			descr: "negative double infinity",
			ins: []in{{
				msg:          ",-inf\r\n",
				msgAsFlatStr: "$4\r\n-inf\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, math.Inf(-1)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase {
				return append(strCases(in, "-inf"), floatCases(in, math.Inf(-1))...)
			},
		},
		{
			descr: "true",
			ins: []in{{
				msg:          "#t\r\n",
				msgAsFlatStr: "$1\r\n1\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, true}, r: true}}
				},
			}},
			// intCases will include actually unmarshaling into a bool
			mkCases: func(in in) []kase { return append(strCases(in, "1"), intCases(in, 1)...) },
		},
		{
			descr: "false",
			ins: []in{{
				msg:          "#f\r\n",
				msgAsFlatStr: "$1\r\n0\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, false}, r: true}}
				},
			}},
			// intCases will include actually unmarshaling into a bool
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "empty blob error",
			ins: []in{{
				msg:          "!0\r\n\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte{}}},
		},
		{
			descr: "blob error",
			ins: []in{{
				msg:          "!4\r\nohey\r\n",
				msgAsFlatStr: "$4\r\nohey\r\n",
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("ohey")}},
		},
		{
			descr: "blob error with delim",
			ins: []in{{
				msg:          "!6\r\noh\r\ney\r\n",
				msgAsFlatStr: "$6\r\noh\r\ney\r\n",
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("oh\r\ney")}},
		},
		{
			descr: "empty verbatim string",
			ins: []in{{
				msg:          "=4\r\ntxt:\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{VerbatimString{}, VerbatimString{Format: "txt"}}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt"},
					}},
					{ie: ie{VerbatimStringBytes{}, VerbatimStringBytes{Format: []byte("txt"), B: []byte{}}}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte{}},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "verbatim string",
			ins: []in{{
				msg:          "=8\r\ntxt:ohey\r\n",
				msgAsFlatStr: "$4\r\nohey\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ohey")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "ohey")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{
						VerbatimString{},
						VerbatimString{Format: "txt", S: "ohey"},
					}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt", S: "ohey"},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("ohey")},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("ohey")},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "verbatim string with delim",
			ins: []in{{
				msg:          "=10\r\ntxt:oh\r\ney\r\n",
				msgAsFlatStr: "$6\r\noh\r\ney\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("oh\r\ney")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "oh\r\ney")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{
						VerbatimString{},
						VerbatimString{Format: "txt", S: "oh\r\ney"},
					}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt", S: "oh\r\ney"},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("oh\r\ney")},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("oh\r\ney")},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "zero big number",
			ins: []in{{
				msg:          "(0\r\n",
				msgAsFlatStr: "$1\r\n0\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "positive big number",
			ins: []in{{
				msg:          "(1000\r\n",
				msgAsFlatStr: "$4\r\n1000\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int).SetInt64(1000)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "1000"), intCases(in, 1000)...) },
		},
		{
			descr: "negative big number",
			ins: []in{{
				msg:          "(-1000\r\n",
				msgAsFlatStr: "$5\r\n-1000\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int).SetInt64(-1000)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-1000"), intCases(in, -1000)...) },
		},
		{
			// only for backwards compatibility
			descr: "null array",
			ins: []in{{
				msg:          "*-1\r\n",
				msgAsFlatStr: "$0\r\n\r\n",
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []interface{}(nil)}, r: false, flatStrEmpty: true}}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "empty agg",
			ins: []in{
				{
					msg:          "*0\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{}}},
							{ie: ie{ArrayHeader{}, ArrayHeader{}}},
							{ie: ie{ArrayHeader{NumElems: 1}, ArrayHeader{}}},
						})
					},
				},
				{
					msg:          "*?\r\n.\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{NumElems: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:          "~0\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]struct{}{}}},
							{ie: ie{SetHeader{}, SetHeader{}}},
							{ie: ie{SetHeader{NumElems: 1}, SetHeader{}}},
						})
					},
				},
				{
					msg:          "~?\r\n.\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]struct{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{NumElems: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:          "%0\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]interface{}{}}},
							{ie: ie{MapHeader{}, MapHeader{}}},
							{ie: ie{MapHeader{NumPairs: 1}, MapHeader{}}},
						})
					},
				},
				{
					msg:          "%?\r\n.\r\n",
					msgAsFlatStr: "",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{NumPairs: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				isArray := prefix == ArrayPrefix
				isSet := prefix == SetPrefix
				isMap := prefix == MapPrefix
				return []kase{
					{r: !streamed && isArray, ie: ie{[][]byte(nil), [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[][]byte{}, [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[][]byte{[]byte("a")}, [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[]string(nil), []string{}}},
					{r: !streamed && isArray, ie: ie{[]string{}, []string{}}},
					{r: !streamed && isArray, ie: ie{[]string{"a"}, []string{}}},
					{r: !streamed && isArray, ie: ie{[]int(nil), []int{}}},
					{r: !streamed && isArray, ie: ie{[]int{}, []int{}}},
					{r: !streamed && isArray, ie: ie{[]int{5}, []int{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}(nil), map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}{}, map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}{"a": {}}, map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}(nil), map[int]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}{}, map[int]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}{1: {}}, map[int]struct{}{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte(nil), map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte{}, map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string]string(nil), map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[string]string{}, map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[string]string{"a": "b"}, map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[int]int(nil), map[int]int{}}},
					{r: !streamed && isMap, ie: ie{map[int]int{}, map[int]int{}}},
					{r: !streamed && isMap, ie: ie{map[int]int{5: 5}, map[int]int{}}},
				}
			},
		},
		{
			descr: "two element agg",
			ins: []in{
				{
					msg:          "*2\r\n$1\r\n1\r\n$3\r\n666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{[]byte("1"), []byte("666")}}},
							{ie: ie{
								msgSeries{&ArrayHeader{}, &BlobString{}, &BlobString{}},
								msgSeries{
									&ArrayHeader{NumElems: 2},
									&BlobString{S: "1"},
									&BlobString{S: "666"},
								},
							}},
						})
					},
				},
				{
					msg:          "*?\r\n$1\r\n1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("1"), int64(666)}}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{},
									&BlobString{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&BlobString{S: "1"},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:          "~2\r\n:1\r\n:666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]struct{}{int64(666): {}, int64(1): {}}}},
							{ie: ie{
								msgSeries{&SetHeader{}, &Number{}, &Number{}},
								msgSeries{
									&SetHeader{NumElems: 2},
									&Number{N: 1},
									&Number{N: 666},
								},
							}},
						})
					},
				},
				{
					msg:          "~?\r\n:1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]struct{}{int64(1): {}, int64(666): {}}}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{},
									&Number{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&Number{N: 1},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:          "%1\r\n:1\r\n:666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]interface{}{int64(1): int64(666)}}},
							{ie: ie{
								msgSeries{&MapHeader{}, &Number{}, &Number{}},
								msgSeries{
									&MapHeader{NumPairs: 1},
									&Number{N: 1},
									&Number{N: 666},
								},
							}},
						})
					},
				},
				{
					msg:          "%?\r\n,1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{float64(1): int64(666)}}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{},
									&Double{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&Double{F: 1},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:          ">2\r\n+1\r\n:666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{"1", int64(666)}}},
							{r: true, ie: ie{
								msgSeries{&PushHeader{}, &SimpleString{}, &Number{}},
								msgSeries{
									&PushHeader{NumElems: 2},
									&SimpleString{S: "1"},
									&Number{N: 666},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				return []kase{
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[][]byte(nil), [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[][]byte{}, [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[][]byte{[]byte("a")}, [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[]string(nil), []string{"1", "666"}}},
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[]string{}, []string{"1", "666"}}},
					{r: !streamed && prefix == ArrayPrefix, ie: ie{[]string{"a"}, []string{"1", "666"}}},
					{ie: ie{[]int(nil), []int{1, 666}}},
					{ie: ie{[]int{}, []int{1, 666}}},
					{ie: ie{[]int{5}, []int{1, 666}}},
					{ie: ie{map[string]struct{}(nil), map[string]struct{}{"666": {}, "1": {}}}},
					{ie: ie{map[string]struct{}{}, map[string]struct{}{"666": {}, "1": {}}}},
					{ie: ie{map[string]struct{}{"a": {}}, map[string]struct{}{"666": {}, "1": {}}}},
					{r: !streamed && prefix == SetPrefix, ie: ie{map[int]struct{}(nil), map[int]struct{}{666: {}, 1: {}}}},
					{r: !streamed && prefix == SetPrefix, ie: ie{map[int]struct{}{}, map[int]struct{}{666: {}, 1: {}}}},
					{r: !streamed && prefix == SetPrefix, ie: ie{map[int]struct{}{1: {}}, map[int]struct{}{666: {}, 1: {}}}},
					{ie: ie{map[string][]byte(nil), map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string][]byte{}, map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string]string(nil), map[string]string{"1": "666"}}},
					{ie: ie{map[string]string{}, map[string]string{"1": "666"}}},
					{ie: ie{map[string]string{"a": "b"}, map[string]string{"1": "666"}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]int(nil), map[int]int{1: 666}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]int{}, map[int]int{1: 666}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]int{5: 5}, map[int]int{1: 666}}},
					{ie: ie{map[string]int(nil), map[string]int{"1": 666}}},
					{ie: ie{map[string]int{}, map[string]int{"1": 666}}},
					{ie: ie{map[string]int{"5": 5}, map[string]int{"1": 666}}},
				}
			},
		},
		{
			descr: "nested two element agg",
			ins: []in{
				{
					label:        "arr-arr",
					msg:          "*1\r\n*2\r\n$1\r\n1\r\n$3\r\n666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{[]interface{}{[]byte("1"), []byte("666")}}}},
						})
					},
				},
				{
					msg:          "*?\r\n*2\r\n$1\r\n1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					label:        "arr-set",
					msg:          "*1\r\n~2\r\n:1\r\n:666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						})
					},
				},
				{
					msg:          "*?\r\n~2\r\n:1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{map[interface{}]struct{}{int64(1): {}, int64(666): {}}}}},
						}
					},
				},
				{
					label:        "arr-map",
					msg:          "*1\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						})
					},
				},
				{
					label:        "arr-map-simple",
					msg:          "*1\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						})
					},
				},
				{
					msg:          "*?\r\n%1\r\n:1\r\n$3\r\n666\r\n.\r\n",
					msgAsFlatStr: "$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				return []kase{
					{r: in.label == "arr-arr", ie: ie{[][][]byte(nil), [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][][]byte{}, [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][][]byte{{}, {[]byte("a")}}, [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string(nil), [][]string{{"1", "666"}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string{}, [][]string{{"1", "666"}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string{{}, {"a"}}, [][]string{{"1", "666"}}}},
					{ie: ie{[][]int(nil), [][]int{{1, 666}}}},
					{ie: ie{[][]int{}, [][]int{{1, 666}}}},
					{ie: ie{[][]int{{7}, {5}}, [][]int{{1, 666}}}},
					{ie: ie{[]map[string]struct{}(nil), []map[string]struct{}{{"666": {}, "1": {}}}}},
					{ie: ie{[]map[string]struct{}{}, []map[string]struct{}{{"666": {}, "1": {}}}}},
					{ie: ie{[]map[string]struct{}{{"a": {}}}, []map[string]struct{}{{"666": {}, "1": {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}(nil), []map[int]struct{}{{666: {}, 1: {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}{}, []map[int]struct{}{{666: {}, 1: {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}{{1: {}}}, []map[int]struct{}{{666: {}, 1: {}}}}},
					{ie: ie{[]map[string][]byte(nil), []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string][]byte{}, []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string][]byte{{}, {"a": []byte("b")}}, []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string]string(nil), []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[string]string{}, []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[string]string{{}, {"a": "b"}}, []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[int]int(nil), []map[int]int{{1: 666}}}},
					{ie: ie{[]map[int]int{}, []map[int]int{{1: 666}}}},
					{ie: ie{[]map[int]int{{4: 2}, {7: 5}}, []map[int]int{{1: 666}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string(nil), []map[int]string{{1: "666"}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string{}, []map[int]string{{1: "666"}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string{{4: "2"}, {7: "5"}}, []map[int]string{{1: "666"}}}},
				}
			},
		},
		{
			descr: "keyed nested two element agg",
			ins: []in{
				{
					msg:          "*2\r\n$2\r\n10\r\n*2\r\n$1\r\n1\r\n:666\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					msg:          "*?\r\n$2\r\n10\r\n*2\r\n$1\r\n1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					msg:          "*2\r\n$2\r\n10\r\n~2\r\n:1\r\n:666\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, []interface{}{
								[]byte("10"),
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						}
					},
				},
				{
					msg:          "*?\r\n$2\r\n10\r\n~2\r\n:1\r\n:666\r\n.\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{
								[]byte("10"),
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						}
					},
				},
				{
					msg:          "%1\r\n:10\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, map[interface{}]interface{}{
								int64(10): map[interface{}]interface{}{int64(1): []byte("666")},
							}}},
						}
					},
				},
				{
					msg:          "%?\r\n:10\r\n%1\r\n:1\r\n$3\r\n666\r\n.\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{
								int64(10): map[interface{}]interface{}{int64(1): []byte("666")},
							}}},
						}
					},
				},
				{
					msg:          ">2\r\n$2\r\n10\r\n*2\r\n$1\r\n1\r\n:666\r\n",
					msgAsFlatStr: "$2\r\n10\r\n$1\r\n1\r\n$3\r\n666\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				return []kase{
					{ie: ie{map[string]map[string][]byte(nil), map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string][]byte{}, map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string][]byte{"foo": {"a": []byte("b")}}, map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string]string(nil), map[string]map[string]string{"10": {"1": "666"}}}},
					{ie: ie{map[string]map[string]string{}, map[string]map[string]string{"10": {"1": "666"}}}},
					{ie: ie{map[string]map[string]string{"foo": {"a": "b"}}, map[string]map[string]string{"10": {"1": "666"}}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]map[int]string(nil), map[int]map[int]string{10: {1: "666"}}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]map[int]string{}, map[int]map[int]string{10: {1: "666"}}}},
					{r: !streamed && prefix == MapPrefix, ie: ie{map[int]map[int]string{777: {4: "2"}}, map[int]map[int]string{10: {1: "666"}}}},
					{ie: ie{map[int]map[int]int(nil), map[int]map[int]int{10: {1: 666}}}},
					{ie: ie{map[int]map[int]int{}, map[int]map[int]int{10: {1: 666}}}},
					{ie: ie{map[int]map[int]int{5: {4: 2}}, map[int]map[int]int{10: {1: 666}}}},
				}
			},
		},
		{
			descr: "agg into struct",
			ins: []in{
				{
					label:        "arr",
					msg:          "*10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
				{
					label:        "map",
					msg:          "%5\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
				{
					label:        "map-exact",
					msg:          "%4\r\n+Foo\r\n:1\r\n+BAZ\r\n$1\r\n2\r\n+Boz\r\n:3\r\n+Biz\r\n$1\r\n4\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
				{
					msg:          "*?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
				{
					msg:          "%?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
				{
					msg:          "~?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
					msgAsFlatStr: "$3\r\nFoo\r\n$1\r\n1\r\n$3\r\nBAZ\r\n$1\r\n2\r\n$3\r\nBoz\r\n$1\r\n3\r\n$3\r\nBiz\r\n$1\r\n4\r\n",
				},
			},
			mkCases: func(in in) []kase {
				isExact := in.label == "map-exact"
				return []kase{
					{r: isExact, ie: ie{testStructA{}, testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructA{}, &testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructA{TestStructInner{bar: 6}, []byte("foo")}, testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructA{TestStructInner{bar: 6}, []byte("foo")}, &testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructB{}, testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructB{}, &testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructB{&TestStructInner{bar: 6}, []byte("foo")}, testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructB{&TestStructInner{bar: 6}, []byte("foo")}, &testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
				}
			},
		},
		{
			descr: "empty streamed string",
			ins: []in{
				{
					msg:          "$?\r\n;0\r\n",
					msgAsFlatStr: "$0\r\n\r\n",
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []byte{}}},
							{r: true, ie: ie{streamedStrRW{}, streamedStrRW{}}},
							{r: true, ie: ie{
								msgSeries{&BlobString{}, &StreamedStringChunk{}},
								msgSeries{&BlobString{StreamedStringHeader: true}, &StreamedStringChunk{S: ""}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobString{S: "foo"}, &StreamedStringChunk{S: "foo"}},
								msgSeries{&BlobString{StreamedStringHeader: true}, &StreamedStringChunk{S: ""}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobStringBytes{}, &StreamedStringChunkBytes{}},
								msgSeries{&BlobStringBytes{StreamedStringHeader: true}, &StreamedStringChunkBytes{B: []byte{}}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobStringBytes{B: []byte("foo")}, &StreamedStringChunkBytes{B: []byte("foo")}},
								msgSeries{&BlobStringBytes{StreamedStringHeader: true}, &StreamedStringChunkBytes{B: []byte{}}},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				return setReversable(false, strCases(in, ""))
			},
		},
		{
			descr: "streamed string",
			ins: []in{
				{
					msg:          "$?\r\n;4\r\nohey\r\n;0\r\n",
					msgAsFlatStr: "$4\r\nohey\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("ohey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "ohey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("ohey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
				{
					msg:          "$?\r\n;2\r\noh\r\n;2\r\ney\r\n;0\r\n",
					msgAsFlatStr: "$4\r\nohey\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("oh"), []byte("ey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "oh"},
									&StreamedStringChunk{S: "ey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("oh")},
									&StreamedStringChunkBytes{B: []byte("ey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
				{
					msg:          "$?\r\n;1\r\no\r\n;1\r\nh\r\n;2\r\ney\r\n;0\r\n",
					msgAsFlatStr: "$4\r\nohey\r\n",
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("o"), []byte("h"), []byte("ey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "o"},
									&StreamedStringChunk{S: "h"},
									&StreamedStringChunk{S: "ey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("o")},
									&StreamedStringChunkBytes{B: []byte("h")},
									&StreamedStringChunkBytes{B: []byte("ey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				cases := setReversable(false, strCases(in, "ohey"))
				cases = append(cases, []kase{
					{ie: ie{nil, []byte("ohey")}},
				}...)
				return cases
			},
		},
	}

	// TODO RawMessage

	for _, umt := range unmarshalMarshalTests {
		t.Run(umt.descr, func(t *testing.T) {
			for i, in := range umt.ins {

				assertMarshals := func(t *testing.T, exp string, i interface{}) {
					t.Logf("%#v -> %q", i, in.msg)
					buf := new(bytes.Buffer)
					assert.NoError(t, (Any{
						I:                    i,
						MarshalDeterministic: true,
					}).MarshalRESP(buf))
					assert.Equal(t, exp, buf.String())
				}

				assertMarshalsFlatStr := func(t *testing.T, exp string, i interface{}) {
					t.Logf("%#v (flattened to blob strings) -> %q", i, in.msgAsFlatStr)
					buf := new(bytes.Buffer)
					assert.NoError(t, (Any{
						I:                    i,
						MarshalDeterministic: true,
						MarshalNoAggHeaders:  true,
						MarshalBlobString:    true,
					}).MarshalRESP(buf))
					assert.Equal(t, exp, buf.String())
				}

				if umt.shouldErr != nil {
					buf := bytes.NewBufferString(in.msg)
					br := bufio.NewReader(buf)
					err := Any{}.UnmarshalRESP(br)
					assert.Equal(t, umt.shouldErr, err)
					assert.Zero(t, br.Buffered())
					assert.Empty(t, buf.Bytes())

					var errConnUsable resp.ErrConnUsable
					assert.True(t, errors.As(err, &errConnUsable))
					assertMarshals(t, in.msg, errConnUsable.Err)
					continue
				}

				t.Run("discard", func(t *testing.T) {
					buf := bytes.NewBufferString(in.msg)
					br := bufio.NewReader(buf)
					err := Any{}.UnmarshalRESP(br)
					assert.NoError(t, err)
					assert.Zero(t, br.Buffered())
					assert.Empty(t, buf.Bytes())
				})

				testName := in.label
				if testName == "" {
					testName = fmt.Sprintf("in%d", i)
				}

				t.Run(testName, func(t *testing.T) {
					t.Run("raw message", func(t *testing.T) {
						buf := bytes.NewBufferString(in.msg)
						br := bufio.NewReader(buf)
						var rm RawMessage
						assert.NoError(t, rm.UnmarshalRESP(br))
						assert.Equal(t, in.msg, string(rm))
						assert.Zero(t, br.Buffered())
						assert.Empty(t, buf.Bytes())
					})

					run := func(withAttr, marshalAsFlatStr bool) func(t *testing.T) {
						return func(t *testing.T) {
							for _, kase := range umt.cases(in) {
								t.Run(kase.label, func(t *testing.T) {
									t.Logf("%q -> %#v", in.msg, kase.ie[0])
									buf := new(bytes.Buffer)
									br := bufio.NewReader(buf)

									// test unmarshaling
									if withAttr {
										AttributeHeader{NumPairs: 2}.MarshalRESP(buf)
										SimpleString{S: "foo"}.MarshalRESP(buf)
										SimpleString{S: "1"}.MarshalRESP(buf)
										SimpleString{S: "bar"}.MarshalRESP(buf)
										SimpleString{S: "2"}.MarshalRESP(buf)
									}
									buf.WriteString(in.msg)

									var intoPtrVal reflect.Value
									if kase.ie[0] == nil {
										intoPtrVal = reflect.ValueOf(&kase.ie[0])
									} else {
										intoOrigVal := reflect.ValueOf(kase.ie[0])
										intoPtrVal = reflect.New(intoOrigVal.Type())
										intoPtrVal.Elem().Set(intoOrigVal)
									}

									err := Any{I: intoPtrVal.Interface()}.UnmarshalRESP(br)
									assert.NoError(t, err)

									into := intoPtrVal.Elem().Interface()
									exp := kase.ie[1]
									switch exp := exp.(type) {
									case *big.Int:
										assert.Zero(t, exp.Cmp(into.(*big.Int)))
									case *big.Float:
										assert.Zero(t, exp.Cmp(into.(*big.Float)))
									case BigNumber:
										assert.Zero(t, exp.I.Cmp(into.(BigNumber).I))
									default:
										assert.Equal(t, exp, into)
									}
									assert.Empty(t, buf.Bytes())
									assert.Zero(t, br.Buffered())

									if kase.r {
										if _, marshaler := exp.(resp.Marshaler); !marshaler && marshalAsFlatStr {
											msgAsFlatStr := in.msgAsFlatStr
											if kase.flatStrEmpty {
												msgAsFlatStr = ""
											}
											assertMarshalsFlatStr(t, msgAsFlatStr, exp)
										} else {
											assertMarshals(t, in.msg, exp)
										}
									}
								})
							}
						}
					}

					t.Run("without attr/marshal", run(false, false))
					t.Run("with attr/marshal", run(true, false))
					t.Run("without attr/marshalFlatStr", run(false, true))
					t.Run("with attr/marshalFlatStr", run(true, true))

					t.Run("numElems", func(t *testing.T) {
						for _, kase := range umt.cases(in) {
							t.Run(kase.label, func(t *testing.T) {
								expNumElems := in.expNumElems()
								if _, ok := kase.ie[1].(resp.Marshaler); ok {
									t.Skip("is a resp.Marshaler")
								} else if kase.flatStrEmpty {
									expNumElems = 0
								}

								t.Logf("numElems(%#v) -> %d", kase.ie[1], expNumElems)
								numElems, err := Any{I: kase.ie[1]}.NumElems()
								assert.NoError(t, err)
								assert.Equal(t, expNumElems, numElems)
							})
						}
					})
				})
			}
		})
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
	(StreamedAggregatedTypeEnd{}).MarshalRESP(buf)

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
	// read element with value 1
	// read element with value 2
	// read element with value 3
	// streamed array ended
}
