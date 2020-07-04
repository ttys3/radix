// Package resp3 implements the upgraded redis RESP3 protocol, a plaintext
// protocol which is also binary safe and backwards compatible with th eoriginal
// RESP2 protocol. Redis uses the RESP protocol to communicate with its clients,
// but there's nothing about the protocol which ties it to redis, it could be
// used for almost anything.
//
// See https://github.com/antirez/RESP3 for more details on the protocol.
package resp3

import (
	"bufio"
	"bytes"
	"encoding"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"sync"

	"errors"

	"github.com/mediocregopher/radix/v3/internal/bytesutil"
	"github.com/mediocregopher/radix/v3/resp"
)

var delim = []byte{'\r', '\n'}

// prefix enumerates the possible RESP types by enumerating the different
// prefixes a RESP message might start with.
type prefix []byte

// Enumeration of each of RESP's message types, each denoted by the prefix which
// is prepended to messages of that type.
//
// In order to determine the type of a message which is being written to a
// *bufio.Reader, without actually consuming it, one can use the Peek method and
// compare it against these values.
var (
	BlobStringPrefix     = []byte{'$'}
	SimpleStringPrefix   = []byte{'+'}
	SimpleErrorPrefix    = []byte{'-'}
	NumberPrefix         = []byte{':'}
	NullPrefix           = []byte{'_'}
	DoublePrefix         = []byte{','}
	BooleanPrefix        = []byte{'#'}
	BlobErrorPrefix      = []byte{'!'}
	VerbatimStringPrefix = []byte{'='}
	BigNumberPrefix      = []byte{'('}

	ArrayPrefix     = []byte{'*'}
	MapPrefix       = []byte{'%'}
	SetPrefix       = []byte{'~'}
	AttributePrefix = []byte{'|'}
	PushPrefix      = []byte{'>'}

	StreamedAggregatedTypeEndPrefix = []byte{'.'}
	StreamedStringChunkPrefix       = []byte{';'}
)

// String formats a prefix into a human-readable name for the type it denotes.
func (p prefix) String() string {
	pStr := string(p)
	switch pStr {
	case string(BlobStringPrefix):
		return "blob-string"
	case string(SimpleStringPrefix):
		return "simple-string"
	case string(SimpleErrorPrefix):
		return "simple-error"
	case string(NumberPrefix):
		return "number"
	case string(NullPrefix):
		return "null"
	case string(DoublePrefix):
		return "double"
	case string(BooleanPrefix):
		return "boolean"
	case string(BlobErrorPrefix):
		return "blob-error"
	case string(VerbatimStringPrefix):
		return "verbatim-string"
	case string(BigNumberPrefix):
		return "big-number"
	case string(ArrayPrefix):
		return "array"
	case string(MapPrefix):
		return "map"
	case string(SetPrefix):
		return "set"
	case string(AttributePrefix):
		return "attribute"
	case string(PushPrefix):
		return "push"
	case string(StreamedAggregatedTypeEndPrefix):
		return "streamed-aggregated-type-end"
	case string(StreamedStringChunkPrefix):
		return "streamed-string-chunk"
	default:
		return pStr
	}
}

var (
	// TODO null values shouldn't be needed here
	nullRESP2Bulktring = []byte("$-1\r\n")
	nullRESP2Array     = []byte("*-1\r\n")
	null               = []byte("_\r\n")
	booleanTrue        = []byte("#t\r\n")
	booleanFalse       = []byte("#f\r\n")
	emptyArray         = []byte("*0\r\n")
	streamHeaderSize   = []byte("?")
	streamAggEnd       = []byte(".\r\n")
)

var bools = [][]byte{
	{'0'},
	{'1'},
}

////////////////////////////////////////////////////////////////////////////////

func discardMulti(br *bufio.Reader, l int) error {
	for i := 0; i < l; i++ {
		if err := (Any{}).UnmarshalRESP(br); err != nil {
			return err
		}
	}
	return nil
}

func discardAttribute(br *bufio.Reader) error {
	var attrHead AttributeHeader
	b, err := br.Peek(len(AttributePrefix))
	if err != nil {
		return err
	} else if !bytes.Equal(b, AttributePrefix) {
		return nil
	} else if err := attrHead.UnmarshalRESP(br); err != nil {
		return nil
	}

	return discardMulti(br, attrHead.NumPairs*2)
}

type errUnexpectedPrefix struct {
	Prefix         []byte
	ExpectedPrefix []byte
}

func (e errUnexpectedPrefix) Error() string {
	return fmt.Sprintf(
		"expected prefix %q, got %q",
		prefix(e.ExpectedPrefix).String(),
		prefix(e.Prefix).String(),
	)
}

// peekAndAssertPrefix will peek at the next incoming redis message and assert
// that it is of the type identified by the given RESP prefix (e.g.
// StringStringPrefix, PushPrefix, etc).
//
// If the message is a RESP error (and that wasn't the intended prefix) then it
// will be unmarshaled into the appropriate error type and returned. If the
// message is of any other type (that isn't the intended prefix) it will be
// discarded and errUnexpectedPrefix will be returned.
//
// peekAndAssertPrefix will discard any preceding attribute message when called
// with discardAttr set
func peekAndAssertPrefix(br *bufio.Reader, expectedPrefix []byte, discardAttr bool) error {
	if discardAttr {
		if err := discardAttribute(br); err != nil {
			return err
		}
	}

	b, err := br.Peek(len(expectedPrefix))
	if err != nil {
		return err
	} else if bytes.Equal(b, expectedPrefix) {
		return nil
	} else if bytes.Equal(b, SimpleErrorPrefix) {
		var respErr SimpleError
		if err := respErr.UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: respErr}
	} else if bytes.Equal(b, BlobErrorPrefix) {
		panic("TODO")
	} else if err := (Any{}).UnmarshalRESP(br); err != nil {
		return err
	}
	return resp.ErrConnUsable{Err: errUnexpectedPrefix{
		Prefix:         b,
		ExpectedPrefix: expectedPrefix,
	}}
}

// like peekAndAssertPrefix, but will consume the prefix if it is the correct
// one as well.
func readAndAssertPrefix(br *bufio.Reader, pref []byte, discardAttr bool) error {
	if err := peekAndAssertPrefix(br, pref, discardAttr); err != nil {
		return err
	}
	_, err := br.Discard(len(pref))
	return err
}

////////////////////////////////////////////////////////////////////////////////

// BlobStringBytes represents the blob string type in the RESP protocol using a
// go byte slice. A B value of nil is an empty string.
//
// BlobStringBytes can also be used as the header message of a streamed string.
// When used in that way it will be followed by one or more BlobStringChunk
// messages, ending in a BlobStringChunk with a zero length.
type BlobStringBytes struct {
	B []byte

	// StreamedStringHeader indicates that this message is the header message of
	// a streamed string. It is mutually exclusive with B.
	StreamedStringHeader bool
}

// MarshalRESP implements the Marshaler method.
func (b BlobStringBytes) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, BlobStringPrefix...)
	if b.StreamedStringHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(len(b.B)), 10)
		*scratch = append(*scratch, delim...)
		*scratch = append(*scratch, b.B...)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BlobStringBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobStringPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if bytes.Equal(byt, streamHeaderSize) {
		b.StreamedStringHeader = true
		return nil
	}

	n, err := bytesutil.ParseInt(byt)
	if err != nil {
		return err
	} else if n == -1 {
		return errors.New("BlobStringBytes does not support unmarshaling RESP2 null bulk string")
	} else if n < 0 {
		return fmt.Errorf("invalid blob string length: %d", n)
	} else if n == 0 {
		b.B = nil
	} else {
		b.B = bytesutil.Expand(b.B, int(n), false)
		if _, err := io.ReadFull(br, b.B); err != nil {
			return err
		}
	}

	if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BlobString represents the blob string type in the RESP protocol using a go
// string.
//
// BlobString can also be used as the header message of a streamed string. When
// used in that way it will be followed by one or more BlobStringChunk messages,
// ending in a BlobStringChunk with a zero length.
type BlobString struct {
	S string

	// StreamedStringHeader indicates that this message is the header message of
	// a streamed string. It is mutually exclusive with S.
	StreamedStringHeader bool
}

// MarshalRESP implements the Marshaler method.
func (b BlobString) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, BlobStringPrefix...)
	if b.StreamedStringHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(len(b.S)), 10)
		*scratch = append(*scratch, delim...)
		*scratch = append(*scratch, b.S...)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BlobString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobStringPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if bytes.Equal(byt, streamHeaderSize) {
		b.StreamedStringHeader = true
		return nil
	}

	n, err := bytesutil.ParseInt(byt)
	if err != nil {
		return err
	} else if n == -1 {
		return errors.New("BlobString does not support unmarshaling RESP2 null bulk string")
	} else if n < 0 {
		return fmt.Errorf("invalid blob string length: %d", n)
	} else if n == 0 {
		b.S = ""
	} else {
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)

		*scratch = bytesutil.Expand(*scratch, int(n), false)
		if _, err := io.ReadFull(br, *scratch); err != nil {
			return err
		}
		b.S = string(*scratch)
	}

	if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// SimpleString represents the simple string type in the RESP protocol.
type SimpleString struct {
	S string
}

// MarshalRESP implements the Marshaler method.
func (ss SimpleString) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, SimpleStringPrefix...)
	*scratch = append(*scratch, ss.S...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (ss *SimpleString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, SimpleStringPrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	}

	ss.S = string(b)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// SimpleError represents the simple error type in the RESP protocol. Note that
// this only represents an actual error message being read/written on the
// stream, it is separate from network or parsing errors. An E value of nil is
// equivalent to an empty error string.
type SimpleError struct {
	E error
}

func (e SimpleError) Error() string {
	return e.E.Error()
}

// MarshalRESP implements the Marshaler method.
func (e SimpleError) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, SimpleErrorPrefix...)
	if e.E != nil {
		*scratch = append(*scratch, e.E.Error()...)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (e *SimpleError) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, SimpleErrorPrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	e.E = errors.New(string(b))
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Number represents the number type in the RESP protocol.
type Number struct {
	N int64
}

// MarshalRESP implements the Marshaler method.
func (n Number) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, NumberPrefix...)
	*scratch = strconv.AppendInt(*scratch, n.N, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (n *Number) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, NumberPrefix, true); err != nil {
		return err
	}
	i, err := bytesutil.ReadIntDelim(br)
	n.N = i
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Null represents the null type in the RESP protocol.
//
// NOTE that this will always marshal to the RESP3 null type, but for
// convenience is also capable of unmarshaling the RESP2 null bulk string and
// null array values.
type Null struct{}

// MarshalRESP implements the Marshaler method.
func (Null) MarshalRESP(w io.Writer) error {
	_, err := w.Write(null)
	return err
}

var resp2NullPrefix = []byte("-1\r")

// UnmarshalRESP implements the Unmarshaler method.
func (*Null) UnmarshalRESP(br *bufio.Reader) error {
	if err := discardAttribute(br); err != nil {
		return err
	}

	prefix, err := br.Peek(1)
	if err != nil {
		return err
	}

	switch prefix[0] {
	case NullPrefix[0]:
		b, err := bytesutil.ReadBytesDelim(br)
		if err != nil {
			return err
		} else if len(b) != 1 {
			return errors.New("malformed null resp")
		}
		return nil

	case ArrayPrefix[0], BlobStringPrefix[0]:
		// no matter what size an array or blob string is it _must_ have at
		// least 4 characters on the wire (prefix+size+delim). So only check
		// that.
		b, err := br.Peek(4)
		if err != nil {
			return err
		} else if !bytes.Equal(b[1:], resp2NullPrefix) {
			if err := (Any{}).UnmarshalRESP(br); err != nil {
				return err
			}
			return resp.ErrConnUsable{Err: errors.New("malformed null resp")}
		}

		// actually consume the message, after all this peeking.
		_, err = bytesutil.ReadBytesDelim(br)
		return err

	default:
		if err := (Any{}).UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: errUnexpectedPrefix{
			Prefix:         prefix,
			ExpectedPrefix: NullPrefix,
		}}
	}
}

////////////////////////////////////////////////////////////////////////////////

// Double represents the double type in the RESP protocol.
type Double struct {
	F float64
}

// MarshalRESP implements the Marshaler method.
func (d Double) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	*scratch = append(*scratch, DoublePrefix...)

	if math.IsInf(d.F, 1) {
		*scratch = append(*scratch, "inf"...)
	} else if math.IsInf(d.F, -1) {
		*scratch = append(*scratch, "-inf"...)
	} else {
		*scratch = strconv.AppendFloat(*scratch, d.F, 'f', -1, 64)
	}

	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	bytesutil.PutBytes(scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (d *Double) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, DoublePrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if d.F, err = strconv.ParseFloat(string(b), 64); err != nil {
		return resp.ErrConnUsable{
			Err: fmt.Errorf("failed to parse double resp %q as float: %w", b, err),
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Boolean represents the boolean type in the RESP protocol.
type Boolean struct {
	B bool
}

// MarshalRESP implements the Marshaler method.
func (b Boolean) MarshalRESP(w io.Writer) error {
	var err error
	if b.B {
		_, err = w.Write(booleanTrue)
	} else {
		_, err = w.Write(booleanFalse)
	}
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *Boolean) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BooleanPrefix, true); err != nil {
		return err
	}
	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if len(byt) != 1 {
		return errors.New("malformed boolean resp")
	} else if byt[0] == 't' {
		b.B = true
	} else if byt[0] == 'f' {
		b.B = false
	} else {
		return errors.New("malformed boolean resp")
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BlobError represents the blob error type in the RESP protocol.
type BlobError struct {
	E error
}

func (e BlobError) Error() string {
	return e.E.Error()
}

// MarshalRESP implements the Marshaler method.
func (e BlobError) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	errStr := e.E.Error()
	*scratch = append(*scratch, BlobErrorPrefix...)
	*scratch = strconv.AppendInt(*scratch, int64(len(errStr)), 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, errStr...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (e *BlobError) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobErrorPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid blob error length: %d", n)
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = bytesutil.Expand(*scratch, int(n), false)
	if _, err := io.ReadFull(br, *scratch); err != nil {
		return err
	} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	e.E = errors.New(string(*scratch))
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// VerbatimStringBytes represents the verbatim string type in the RESP protocol
// using a go byte slice. A B value of nil is an empty string.
type VerbatimStringBytes struct {
	B []byte

	// Format is a 3 character string describing the format that the verbatim
	// string is encoded in, e.g. "txt" or "mkd". MarshalRESP will error without
	// writing anything if this is not exactly 3 characters.
	Format []byte
}

// MarshalRESP implements the Marshaler method.
func (b VerbatimStringBytes) MarshalRESP(w io.Writer) error {
	if len(b.Format) != 3 {
		return resp.ErrConnUsable{
			Err: errors.New("Format field must be exactly 3 characters"),
		}
	}
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, VerbatimStringPrefix...)
	*scratch = strconv.AppendInt(*scratch, int64(len(b.B))+4, 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, b.Format...)
	*scratch = append(*scratch, ':')
	*scratch = append(*scratch, b.B...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *VerbatimStringBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, VerbatimStringPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 4 {
		return fmt.Errorf("invalid verbatim string length: %d", n)
	}

	b.B = bytesutil.Expand(b.B, int(n), false)
	if _, err := io.ReadFull(br, b.B); err != nil {
		return err
	} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	b.Format, b.B = b.B[:3], b.B[4:]
	if len(b.B) == 0 {
		b.B = nil
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// VerbatimString represents the verbatim string type in the RESP protocol
// using a go string.
type VerbatimString struct {
	S string

	// Format is a 3 character string describing the format that the verbatim
	// string is encoded in, e.g. "txt" or "mkd". MarshalRESP will error without
	// writing anything if this is not exactly 3 characters.
	Format string
}

// MarshalRESP implements the Marshaler method.
func (b VerbatimString) MarshalRESP(w io.Writer) error {
	if len(b.Format) != 3 {
		return resp.ErrConnUsable{
			Err: errors.New("Format field must be exactly 3 characters"),
		}
	}
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, VerbatimStringPrefix...)
	*scratch = strconv.AppendInt(*scratch, int64(len(b.S))+4, 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, b.Format...)
	*scratch = append(*scratch, ':')
	*scratch = append(*scratch, b.S...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *VerbatimString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, VerbatimStringPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 4 {
		return fmt.Errorf("invalid verbatim string length: %d", n)
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = bytesutil.Expand(*scratch, int(n), false)
	if _, err := io.ReadFull(br, *scratch); err != nil {
		return err
	}

	b.Format = string((*scratch)[:3])
	b.S = string((*scratch)[4:])
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BigNumber represents the big number type in the RESP protocol. Marshaling a
// nil I value will cause a panic.
type BigNumber struct {
	I *big.Int
}

// MarshalRESP implements the Marshaler method.
func (b BigNumber) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, BigNumberPrefix...)
	*scratch = b.I.Append(*scratch, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BigNumber) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BigNumberPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if b.I == nil {
		b.I = new(big.Int)
	}

	var ok bool
	if b.I, ok = b.I.SetString(string(byt), 10); !ok {
		return fmt.Errorf("malformed big number: %q", byt)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BulkReader is like BlobString, but it only supports marshalling and will use
// the given LenReader to do so. If LR is nil then the nil bulk string RESP will
// be written
type BulkReader struct {
	LR resp.LenReader
}

// MarshalRESP implements the Marshaler method.
func (b BulkReader) MarshalRESP(w io.Writer) error {
	if b.LR == nil {
		_, err := w.Write(nullRESP2Bulktring)
		return err
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := b.LR.Len()
	*scratch = append(*scratch, BlobStringPrefix...)
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	if err != nil {
		return err
	}

	if _, err := io.CopyN(w, b.LR, l); err != nil {
		return err
	} else if _, err := w.Write(delim); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func marshalAggHeader(w io.Writer, prefix []byte, n int, streamHeader bool) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, prefix...)
	if streamHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(n), 10)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

func unmarshalAggHeader(br *bufio.Reader, prefix []byte, n *int, streamHeader *bool, discardAttr bool) error {
	if err := readAndAssertPrefix(br, prefix, discardAttr); err != nil {
		return err
	}

	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if streamHeader != nil {
		if *streamHeader = bytes.Equal(b, streamHeaderSize); *streamHeader {
			return nil
		}
	}

	n64, err := bytesutil.ParseInt(b)
	if err != nil {
		return err
	} else if n64 < 0 {
		return fmt.Errorf("invalid number of elements: %d", n64)
	}

	*n = int(n64)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// ArrayHeader represents the header sent preceding array elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// ArrayHeader can also be used as the header of a streamed array, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type ArrayHeader struct {
	NumElems int

	// StreamedArrayHeader indicates that this message is the header message of
	// a streamed array. It is mutually exclusive with NumElems.
	StreamedArrayHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h ArrayHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, ArrayPrefix, h.NumElems, h.StreamedArrayHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *ArrayHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, ArrayPrefix, &h.NumElems, &h.StreamedArrayHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// MapHeader represents the header sent preceding map key/value pairs in the
// RESP protocol. It does not actually encompass any pairs itself, it only
// declares how many pairs will come after it.
//
// MapHeader can also be used as the header of a streamed map, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type MapHeader struct {
	NumPairs int

	// StreamedMapHeader indicates that this message is the header message of
	// a streamed map. It is mutually exclusive with NumPairs.
	StreamedMapHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h MapHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, MapPrefix, h.NumPairs, h.StreamedMapHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *MapHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, MapPrefix, &h.NumPairs, &h.StreamedMapHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// SetHeader represents the header sent preceding set elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// SetHeader can also be used as the header of a streamed set, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type SetHeader struct {
	NumElems int

	// StreamedSetHeader indicates that this message is the header message of
	// a streamed set. It is mutually exclusive with NumElems.
	StreamedSetHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h SetHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, SetPrefix, h.NumElems, h.StreamedSetHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *SetHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, SetPrefix, &h.NumElems, &h.StreamedSetHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// AttributeHeader represents the header sent preceding attribute key/value
// pairs in the RESP protocol. It does not actually encompass any pairs itself,
// it only declares how many pairs will come after it.
type AttributeHeader struct {
	NumPairs int
}

// MarshalRESP implements the Marshaler method.
func (h AttributeHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, AttributePrefix, h.NumPairs, false)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *AttributeHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, AttributePrefix, &h.NumPairs, nil, false)
}

////////////////////////////////////////////////////////////////////////////////

// PushHeader represents the header sent preceding push elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
type PushHeader struct {
	NumElems int
}

// MarshalRESP implements the Marshaler method.
func (h PushHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, PushPrefix, h.NumElems, false)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *PushHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, PushPrefix, &h.NumElems, nil, true)
}

////////////////////////////////////////////////////////////////////////////////

// StreamedStringChunkBytes represents a chunk of a streamed string in the RESP
// protocol using a byte slice. An empty string indicates the end of the
// streamed string. A B value of nil is an empty string.
type StreamedStringChunkBytes struct {
	B []byte
}

// MarshalRESP implements the Marshaler method.
func (b StreamedStringChunkBytes) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := int64(len(b.B))
	*scratch = append(*scratch, StreamedStringChunkPrefix...)
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	if l > 0 {
		*scratch = append(*scratch, b.B...)
		*scratch = append(*scratch, delim...)
	}
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *StreamedStringChunkBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, StreamedStringChunkPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid streamed string chunk length: %d", n)
	} else if n == 0 {
		b.B = nil
	} else {
		b.B = bytesutil.Expand(b.B, int(n), false)
		if _, err := io.ReadFull(br, b.B); err != nil {
			return err
		} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// StreamedStringChunk represents a chunk of a streamed string in the RESP
// protocol using a go string. An empty string indicates the end of the streamed
// string.
type StreamedStringChunk struct {
	S string
}

// MarshalRESP implements the Marshaler method.
func (b StreamedStringChunk) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := int64(len(b.S))
	*scratch = append(*scratch, StreamedStringChunkPrefix...)
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	if l > 0 {
		*scratch = append(*scratch, b.S...)
		*scratch = append(*scratch, delim...)
	}
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *StreamedStringChunk) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, StreamedStringChunkPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid streamed string chunk length: %d", n)
	} else if n == 0 {
		b.S = ""
	} else {
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)

		*scratch = bytesutil.Expand(*scratch, int(n), false)
		if _, err := io.ReadFull(br, *scratch); err != nil {
			return err
		} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
			return err
		}
		b.S = string(*scratch)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// StreamedAggregatedElement is used to unmarshal the elements of a streamed
// aggregated type such that it is possible to check if the end of the stream
// has been reached.
type StreamedAggregatedElement struct {
	// Unmarshaler is used when the message being read isn't the streamed
	// aggregated end type.
	resp.Unmarshaler

	// End is used when UnmarshalRESP is called to indicate that the message
	// read was the streamed aggregated end type (".\r\n") and so the end of the
	// stream has been reached.. If this is true then UnmarshalRESP was not
	// called on the Unmarshaler field.
	End bool
}

// UnmarshalRESP implements the Unmarshaler method.
func (s *StreamedAggregatedElement) UnmarshalRESP(br *bufio.Reader) error {
	b, err := br.Peek(len(streamAggEnd))
	if err != nil {
		return err
	} else if s.End = bytes.Equal(b, streamAggEnd); s.End {
		_, err = br.Discard(len(b))
		return err
	}
	return s.Unmarshaler.UnmarshalRESP(br)
}

// StreamedAggregatedEnd is used to marshal the streamed aggregated end type
// message.
type StreamedAggregatedEnd struct{}

// MarshalRESP implements the Marshaler method.
func (s StreamedAggregatedEnd) MarshalRESP(w io.Writer) error {
	_, err := w.Write(streamAggEnd)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func discardMultiAfterErr(br *bufio.Reader, left int, err error) error {
	// if the last error which occurred didn't discard the message it was on, we
	// can't do anything
	if !errors.As(err, new(resp.ErrConnUsable)) {
		return err
	} else if err := discardMulti(br, left); err != nil {
		return err
	}

	// The original error was already wrapped in an ErrConnUsable, so just return
	// it as it was given
	return err
}

// Any represents any primitive go type, such as integers, floats, strings,
// bools, etc... It also includes encoding.Text(Un)Marshalers and
// encoding.(Un)BinaryMarshalers. It will _not_ marshal resp.Marshalers.
//
// Most things will be treated as bulk strings, except for those that have their
// own corresponding type in the RESP protocol (e.g. ints). strings and []bytes
// will always be encoded as bulk strings, never simple strings.
//
// Arrays and slices will be treated as RESP arrays, and their values will be
// treated as if also wrapped in an Any struct. Maps will be similarly treated,
// but they will be flattened into arrays of their alternating keys/values
// first.
//
// When using UnmarshalRESP the value of I must be a pointer or nil. If it is
// nil then the RESP value will be read and discarded.
//
// If an error type is read in the UnmarshalRESP method then a resp2.Error will
// be returned with that error, and the value of I won't be touched.
type Any struct {
	I interface{}

	// If true then the MarshalRESP method will marshal all non-array types as
	// bulk strings. This primarily effects integers and errors.
	MarshalBlobString bool

	// If true then no array headers will be sent when MarshalRESP is called.
	// For I values which are non-arrays this means no behavior change. For
	// arrays and embedded arrays it means only the array elements will be
	// written, and an ArrayHeader must have been manually marshalled
	// beforehand.
	MarshalNoArrayHeaders bool
}

func (a Any) cp(i interface{}) Any {
	a.I = i
	return a
}

var byteSliceT = reflect.TypeOf([]byte{})

// NumElems returns the number of non-array elements which would be marshalled
// based on I. For example:
//
//	Any{I: "foo"}.NumElems() == 1
//	Any{I: []string{}}.NumElems() == 0
//	Any{I: []string{"foo"}}.NumElems() == 1
//	Any{I: []string{"foo", "bar"}}.NumElems() == 2
//	Any{I: [][]string{{"foo"}, {"bar", "baz"}, {}}}.NumElems() == 3
//
func (a Any) NumElems() int {
	return numElems(reflect.ValueOf(a.I))
}

var (
	lenReaderT               = reflect.TypeOf(new(resp.LenReader)).Elem()
	encodingTextMarshalerT   = reflect.TypeOf(new(encoding.TextMarshaler)).Elem()
	encodingBinaryMarshalerT = reflect.TypeOf(new(encoding.BinaryMarshaler)).Elem()
)

func numElems(vv reflect.Value) int {
	if !vv.IsValid() {
		return 1
	}

	tt := vv.Type()
	switch {
	case tt.Implements(lenReaderT):
		return 1
	case tt.Implements(encodingTextMarshalerT):
		return 1
	case tt.Implements(encodingBinaryMarshalerT):
		return 1
	}

	switch vv.Kind() {
	case reflect.Ptr:
		return numElems(reflect.Indirect(vv))
	case reflect.Slice, reflect.Array:
		// TODO does []rune need extra support here?
		if vv.Type() == byteSliceT {
			return 1
		}

		l := vv.Len()
		var c int
		for i := 0; i < l; i++ {
			c += numElems(vv.Index(i))
		}
		return c

	case reflect.Map:
		kkv := vv.MapKeys()
		var c int
		for _, kv := range kkv {
			c += numElems(kv)
			c += numElems(vv.MapIndex(kv))
		}
		return c

	case reflect.Interface:
		return numElems(vv.Elem())

	case reflect.Struct:
		return numElemsStruct(vv, true)

	default:
		return 1
	}
}

// this is separated out of numElems because marshalStruct is only given the
// reflect.Value and needs to know the numElems, so it wouldn't make sense to
// recast to an interface{} to pass into NumElems, it would just get turned into
// a reflect.Value again.
func numElemsStruct(vv reflect.Value, flat bool) int {
	tt := vv.Type()
	l := vv.NumField()
	var c int
	for i := 0; i < l; i++ {
		ft, fv := tt.Field(i), vv.Field(i)
		if ft.Anonymous {
			if fv = reflect.Indirect(fv); fv.IsValid() { // fv isn't nil
				c += numElemsStruct(fv, flat)
			}
			continue
		} else if ft.PkgPath != "" || ft.Tag.Get("redis") == "-" {
			continue // continue
		}

		c++ // for the key
		if flat {
			c += numElems(fv)
		} else {
			c++
		}
	}
	return c
}

// MarshalRESP implements the Marshaler method.
func (a Any) MarshalRESP(w io.Writer) error {

	marshalBlobStr := func(b []byte) error {
		return BlobStringBytes{B: b}.MarshalRESP(w)
	}

	switch at := a.I.(type) {
	case []byte:
		return marshalBlobStr(at)
	case string:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = append(*scratch, at...)
		return marshalBlobStr(*scratch)
	case bool:
		b := bools[0]
		if at {
			b = bools[1]
		}
		return marshalBlobStr(b)
	case float32:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, float64(at), 'f', -1, 32)
		return marshalBlobStr(*scratch)
	case float64:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, at, 'f', -1, 64)
		return marshalBlobStr(*scratch)
	case nil:
		panic("TODO")
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		at64 := bytesutil.AnyIntToInt64(at)
		if a.MarshalBlobString {
			scratch := bytesutil.GetBytes()
			defer bytesutil.PutBytes(scratch)
			*scratch = strconv.AppendInt(*scratch, at64, 10)
			return marshalBlobStr(*scratch)
		}
		return Number{N: at64}.MarshalRESP(w)
	case error:
		if a.MarshalBlobString {
			scratch := bytesutil.GetBytes()
			defer bytesutil.PutBytes(scratch)
			*scratch = append(*scratch, at.Error()...)
			return marshalBlobStr(*scratch)
		}
		// TODO or blob error?
		return SimpleError{E: at}.MarshalRESP(w)
	case resp.LenReader:
		return BulkReader{LR: at}.MarshalRESP(w)
	case encoding.TextMarshaler:
		b, err := at.MarshalText()
		if err != nil {
			return err
		}
		return marshalBlobStr(b)
	case encoding.BinaryMarshaler:
		b, err := at.MarshalBinary()
		if err != nil {
			return err
		}
		return marshalBlobStr(b)
	}

	// now we use.... reflection! duhduhduuuuh....
	vv := reflect.ValueOf(a.I)

	// if it's a pointer we de-reference and try the pointed to value directly
	if vv.Kind() == reflect.Ptr {
		var ivv reflect.Value
		if vv.IsNil() {
			ivv = reflect.New(vv.Type().Elem())
		} else {
			ivv = reflect.Indirect(vv)
		}
		return a.cp(ivv.Interface()).MarshalRESP(w)
	}

	// some helper functions
	var err error
	var anyWritten bool
	setAnyWritten := func() {
		var errConnUsable resp.ErrConnUsable
		if !errors.As(err, &errConnUsable) {
			anyWritten = true
		}
	}
	arrHeader := func(l int) {
		if a.MarshalNoArrayHeaders || err != nil {
			return
		}
		err = ArrayHeader{NumElems: l}.MarshalRESP(w)
		setAnyWritten()
	}
	arrVal := func(v interface{}) {
		if err != nil {
			return
		}
		err = a.cp(v).MarshalRESP(w)
		setAnyWritten()
	}
	unwrapIfAnyWritten := func() {
		if anyWritten {
			err = resp.ErrConnUnusable(err)
		}
	}

	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			_, err := w.Write(nullRESP2Array)
			return err
		}
		l := vv.Len()
		arrHeader(l)
		for i := 0; i < l; i++ {
			arrVal(vv.Index(i).Interface())
		}
		unwrapIfAnyWritten()

	case reflect.Map:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			_, err := w.Write(nullRESP2Array)
			return err
		}
		kkv := vv.MapKeys()
		arrHeader(len(kkv) * 2)
		for _, kv := range kkv {
			arrVal(kv.Interface())
			arrVal(vv.MapIndex(kv).Interface())
		}
		unwrapIfAnyWritten()

	case reflect.Struct:
		return a.marshalStruct(w, vv, false)

	default:
		return resp.ErrConnUsable{
			Err: fmt.Errorf("could not marshal value of type %T", a.I),
		}
	}

	return err
}

func (a Any) marshalStruct(w io.Writer, vv reflect.Value, inline bool) error {
	var err error
	if !a.MarshalNoArrayHeaders && !inline {
		numElems := numElemsStruct(vv, a.MarshalNoArrayHeaders)
		if err = (ArrayHeader{NumElems: numElems}).MarshalRESP(w); err != nil {
			return err
		}
	}

	tt := vv.Type()
	l := vv.NumField()
	for i := 0; i < l; i++ {
		ft, fv := tt.Field(i), vv.Field(i)
		tag := ft.Tag.Get("redis")
		if ft.Anonymous {
			if fv = reflect.Indirect(fv); !fv.IsValid() { // fv is nil
				continue
			} else if err := a.marshalStruct(w, fv, true); err != nil {
				return err
			}
			continue
		} else if ft.PkgPath != "" || tag == "-" {
			continue // unexported
		}

		keyName := ft.Name
		if tag != "" {
			keyName = tag
		}
		if err := (BlobString{S: keyName}).MarshalRESP(w); err != nil {
			return err
		} else if err := a.cp(fv.Interface()).MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

func saneDefault(prefix byte) interface{} {
	// we don't handle ErrorPrefix because that always returns an error and
	// doesn't touch I
	switch prefix {
	case ArrayPrefix[0]:
		ii := make([]interface{}, 8)
		return &ii
	case BlobStringPrefix[0]:
		bb := make([]byte, 16)
		return &bb
	case SimpleStringPrefix[0]:
		return new(string)
	case NumberPrefix[0]:
		return new(int64)
	}
	panic("should never get here")
}

// We use pools for these even though they only get used within
// Any.UnmarshalRESP because of how often they get used. Any return from redis
// which has a simple string or bulk string (the vast majority of them) is going
// to go through one of these.
var (
	// RawMessage.UnmarshalInto also uses these
	byteReaderPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewReader(nil)
		},
	}
	bufioReaderPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReader(nil)
		},
	}
)

// UnmarshalRESP implements the Unmarshaler method.
func (a Any) UnmarshalRESP(br *bufio.Reader) error {
	// if I is itself an Unmarshaler just hit that directly
	if u, ok := a.I.(resp.Unmarshaler); ok {
		return u.UnmarshalRESP(br)
	}

	b, err := br.Peek(1)
	if err != nil {
		return err
	}
	prefix := b[0]

	// This is a super special case that _must_ be handled before we actually
	// read from the reader. If an *interface{} is given we instead unmarshal
	// into a default (created based on the type of th message), then set the
	// *interface{} to that
	if ai, ok := a.I.(*interface{}); ok {
		innerA := Any{I: saneDefault(prefix)}
		if err := innerA.UnmarshalRESP(br); err != nil {
			return err
		}
		*ai = reflect.ValueOf(innerA.I).Elem().Interface()
		return nil
	}

	br.Discard(1)
	b, err = bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	}

	switch prefix {
	case SimpleErrorPrefix[0]:
		return resp.ErrConnUsable{Err: SimpleError{E: errors.New(string(b))}}
	case ArrayPrefix[0]:
		l, err := bytesutil.ParseInt(b)
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		return a.unmarshalArray(br, l)
	case BlobStringPrefix[0]:
		l, err := bytesutil.ParseInt(b) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}

		// This is a bit of a clusterfuck. Basically:
		// - If unmarshal returns a non-ErrConnUsable error, return that asap.
		// - If discarding the last 2 bytes (in order to discard the full
		//   message) fails, return that asap
		// - Otherwise return the original error, if there was any
		if err = a.unmarshalSingle(br, int(l)); err != nil {
			if !errors.As(err, new(resp.ErrConnUsable)) {
				return err
			}
		}
		if _, discardErr := br.Discard(2); discardErr != nil {
			return discardErr
		}
		return err
	case SimpleStringPrefix[0], NumberPrefix[0]:
		reader := byteReaderPool.Get().(*bytes.Reader)
		reader.Reset(b)
		err := a.unmarshalSingle(reader, reader.Len())
		byteReaderPool.Put(reader)
		return err
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

func (a Any) unmarshalSingle(body io.Reader, n int) error {
	var (
		err error
		i   int64
		ui  uint64
	)

	switch ai := a.I.(type) {
	case nil:
		// just read it and do nothing
		err = bytesutil.ReadNDiscard(body, n)
	case *string:
		scratch := bytesutil.GetBytes()
		*scratch, err = bytesutil.ReadNAppend(body, *scratch, n)
		*ai = string(*scratch)
		bytesutil.PutBytes(scratch)
	case *[]byte:
		*ai, err = bytesutil.ReadNAppend(body, (*ai)[:0], n)
	case *bool:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = ui > 0
	case *int:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int(i)
	case *int8:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int8(i)
	case *int16:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int16(i)
	case *int32:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int32(i)
	case *int64:
		i, err = bytesutil.ReadInt(body, n)
		*ai = i
	case *uint:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint(ui)
	case *uint8:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint8(ui)
	case *uint16:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint16(ui)
	case *uint32:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint32(ui)
	case *uint64:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = ui
	case *float32:
		var f float64
		f, err = bytesutil.ReadFloat(body, 32, n)
		*ai = float32(f)
	case *float64:
		*ai, err = bytesutil.ReadFloat(body, 64, n)
	case io.Writer:
		_, err = io.CopyN(ai, body, int64(n))
	case encoding.TextUnmarshaler:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalText(*scratch)
	case encoding.BinaryUnmarshaler:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalBinary(*scratch)
	default:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
			break
		}
		err = resp.ErrConnUsable{
			Err: fmt.Errorf("can't unmarshal into %T, message body was: %q", a.I, *scratch),
		}
	}

	return err
}

func (a Any) unmarshalNil() error {
	vv := reflect.ValueOf(a.I)
	if vv.Kind() != reflect.Ptr || !vv.Elem().CanSet() {
		// If the type in I can't be set then just ignore it. This is kind of
		// weird but it's what encoding/json does in the same circumstance
		return nil
	}

	vve := vv.Elem()
	vve.Set(reflect.Zero(vve.Type()))
	return nil
}

func (a Any) unmarshalArray(br *bufio.Reader, l int64) error {
	if a.I == nil {
		return discardMulti(br, int(l))
	}

	size := int(l)
	v := reflect.ValueOf(a.I)
	if v.Kind() != reflect.Ptr {
		err := resp.ErrConnUsable{
			Err: fmt.Errorf("can't unmarshal array into %T", a.I),
		}
		return discardMultiAfterErr(br, int(l), err)
	}
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Slice:
		if size > v.Cap() || v.IsNil() {
			newV := reflect.MakeSlice(v.Type(), size, size)
			// we copy only because there might be some preset values in there
			// already that we're intended to decode into,
			// e.g.  []interface{}{int8(0), ""}
			reflect.Copy(newV, v)
			v.Set(newV)
		} else if size != v.Len() {
			v.SetLen(size)
		}

		for i := 0; i < size; i++ {
			ai := Any{I: v.Index(i).Addr().Interface()}
			if err := ai.UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-1, err)
			}
		}
		return nil

	case reflect.Map:
		if size%2 != 0 {
			err := resp.ErrConnUsable{Err: errors.New("cannot decode redis array with odd number of elements into map")}
			return discardMultiAfterErr(br, int(l), err)
		} else if v.IsNil() {
			v.Set(reflect.MakeMapWithSize(v.Type(), size/2))
		}

		var kvs reflect.Value
		if size > 0 && canShareReflectValue(v.Type().Key()) {
			kvs = reflect.New(v.Type().Key())
		}

		var vvs reflect.Value
		if size > 0 && canShareReflectValue(v.Type().Elem()) {
			vvs = reflect.New(v.Type().Elem())
		}

		for i := 0; i < size; i += 2 {
			kv := kvs
			if !kv.IsValid() {
				kv = reflect.New(v.Type().Key())
			}
			if err := (Any{I: kv.Interface()}).UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-1, err)
			}

			vv := vvs
			if !vv.IsValid() {
				vv = reflect.New(v.Type().Elem())
			}
			if err := (Any{I: vv.Interface()}).UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-2, err)
			}

			v.SetMapIndex(kv.Elem(), vv.Elem())
		}
		return nil

	case reflect.Struct:
		if size%2 != 0 {
			err := resp.ErrConnUsable{Err: errors.New("cannot decode redis array with odd number of elements into struct")}
			return discardMultiAfterErr(br, int(l), err)
		}

		structFields := getStructFields(v.Type())
		var field BlobStringBytes

		for i := 0; i < size; i += 2 {
			if err := field.UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-1, err)
			}

			var vv reflect.Value
			structField, ok := structFields[string(field.B)] // no allocation, since Go 1.3
			if ok {
				vv = getStructField(v, structField.indices)
			}

			if !ok || !vv.IsValid() {
				// discard the value
				if err := (Any{}).UnmarshalRESP(br); err != nil {
					return discardMultiAfterErr(br, int(l)-i-2, err)
				}
				continue
			}

			if err := (Any{I: vv.Interface()}).UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-2, err)
			}
		}

		return nil

	default:
		err := resp.ErrConnUsable{Err: fmt.Errorf("cannot decode redis array into %v", v.Type())}
		return discardMultiAfterErr(br, int(l), err)
	}
}

func canShareReflectValue(ty reflect.Type) bool {
	switch ty.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String:
		return true
	default:
		return false
	}
}

type structField struct {
	name    string
	fromTag bool // from a tag overwrites a field name
	indices []int
}

// encoding/json uses a similar pattern for unmarshaling into structs
var structFieldsCache sync.Map // aka map[reflect.Type]map[string]structField

func getStructFields(t reflect.Type) map[string]structField {
	if mV, ok := structFieldsCache.Load(t); ok {
		return mV.(map[string]structField)
	}

	getIndices := func(parents []int, i int) []int {
		indices := make([]int, len(parents), len(parents)+1)
		copy(indices, parents)
		indices = append(indices, i)
		return indices
	}

	m := map[string]structField{}

	var populateFrom func(reflect.Type, []int)
	populateFrom = func(t reflect.Type, parents []int) {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		l := t.NumField()

		// first get all fields which aren't embedded structs
		for i := 0; i < l; i++ {
			ft := t.Field(i)
			if ft.Anonymous || ft.PkgPath != "" {
				continue
			}

			key, fromTag := ft.Name, false
			if tag := ft.Tag.Get("redis"); tag != "" && tag != "-" {
				key, fromTag = tag, true
			}
			if m[key].fromTag {
				continue
			}
			m[key] = structField{
				name:    key,
				fromTag: fromTag,
				indices: getIndices(parents, i),
			}
		}

		// then find all embedded structs and descend into them
		for i := 0; i < l; i++ {
			ft := t.Field(i)
			if !ft.Anonymous {
				continue
			}
			populateFrom(ft.Type, getIndices(parents, i))
		}
	}

	populateFrom(t, []int{})
	structFieldsCache.LoadOrStore(t, m)
	return m
}

// v must be setable. Always returns a Kind() == reflect.Ptr, unless it returns
// the zero Value, which means a setable value couldn't be gotten.
func getStructField(v reflect.Value, ii []int) reflect.Value {
	if len(ii) == 0 {
		return v.Addr()
	}
	i, ii := ii[0], ii[1:]

	iv := v.Field(i)
	if iv.Kind() == reflect.Ptr && iv.IsNil() {
		// If the field is a pointer to an unexported type then it won't be
		// settable, though if the user pre-sets the value it will be (I think).
		if !iv.CanSet() {
			return reflect.Value{}
		}
		iv.Set(reflect.New(iv.Type().Elem()))
	}
	iv = reflect.Indirect(iv)

	return getStructField(iv, ii)
}

////////////////////////////////////////////////////////////////////////////////

// RawMessage is a Marshaler/Unmarshaler which will capture the exact raw bytes
// of a RESP message. When Marshaling the exact bytes of the RawMessage will be
// written as-is. When Unmarshaling the bytes of a single RESP message will be
// read into the RawMessage's bytes.
type RawMessage []byte

// MarshalRESP implements the Marshaler method.
func (rm RawMessage) MarshalRESP(w io.Writer) error {
	_, err := w.Write(rm)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (rm *RawMessage) UnmarshalRESP(br *bufio.Reader) error {
	*rm = (*rm)[:0]
	return rm.unmarshal(br)
}

func (rm *RawMessage) unmarshal(br *bufio.Reader) error {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return err
	}
	*rm = append(*rm, b...)

	if len(b) < 3 {
		return errors.New("malformed data read")
	}
	body := b[1 : len(b)-2]

	switch b[0] {
	case ArrayPrefix[0]:
		l, err := bytesutil.ParseInt(body)
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		for i := 0; i < int(l); i++ {
			if err := rm.unmarshal(br); err != nil {
				return err
			}
		}
		return nil
	case BlobStringPrefix[0]:
		l, err := bytesutil.ParseInt(body) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		*rm, err = bytesutil.ReadNAppend(br, *rm, int(l+2))
		return err
	case SimpleErrorPrefix[0], SimpleStringPrefix[0], NumberPrefix[0]:
		return nil
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

// UnmarshalInto is a shortcut for wrapping this RawMessage in a *bufio.Reader
// and passing that into the given Unmarshaler's UnmarshalRESP method. Any error
// from calling UnmarshalRESP is returned, and the RawMessage is unaffected in
// all cases.
func (rm RawMessage) UnmarshalInto(u resp.Unmarshaler) error {
	r := byteReaderPool.Get().(*bytes.Reader)
	r.Reset(rm)
	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(r)
	err := u.UnmarshalRESP(br)
	bufioReaderPool.Put(br)
	byteReaderPool.Put(r)
	return err
}

// IsNil returns true if the contents of RawMessage are one of the nil values.
func (rm RawMessage) IsNil() bool {
	// TODO this is wrong
	return bytes.Equal(rm, nullRESP2Bulktring) || bytes.Equal(rm, nullRESP2Array)
}

// IsEmptyArray returns true if the contents of RawMessage is empty array value.
func (rm RawMessage) IsEmptyArray() bool {
	return bytes.Equal(rm, emptyArray)
}
