// Pacakge protocol implements mongodb wire protocol
package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

type Op interface {
	MessageLength() uint32
	RequestID() uint32
	ResponseTo() uint32
	OpCode() OpCode
	data() []byte
}

//go:generate stringer --type OpCode

type OpCode uint32

const (
	OpReply        OpCode = 1    // Reply to a client request. responseTo is set.
	OpUpdate       OpCode = 2001 // Update document.
	OpInsert       OpCode = 2002 // Insert new document.
	Reserved       OpCode = 2003 // Formerly used for OPGetByOid.
	OpQuery        OpCode = 2004 // Query a collection.
	OpGetMore      OpCode = 2005 // Get more data from a query. See Cursors.
	OpDelete       OpCode = 2006 // Delete documents.
	OpKillCursors  OpCode = 2007 // Notify database that the client has finished with the cursor.
	OpCommand      OpCode = 2010 // Cluster internal protocol representing a command request.
	OpCommandreply OpCode = 2011 // Cluster internal protocol representing a reply to an OPCommand.
	OpMsg          OpCode = 2013 // Send a message using the format introduced in MongoDB 3.6.
)

type Decoder struct {
	r io.Reader
}

func NewDecoder(r io.Reader) Decoder {
	return Decoder{r: r}
}

func (d Decoder) Decode() (Op, error) {
	var lengthData [4]byte
	if _, err := d.r.Read(lengthData[:]); err != nil {
		return nil, err
	}
	length := le.Uint32(lengthData[:])
	bodyData := make([]byte, length)
	copy(bodyData, lengthData[:])
	if _, err := io.ReadFull(d.r, bodyData[4:]); err != nil {
		return nil, err
	}
	return Parse(bodyData)
}

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) Encoder {
	return Encoder{w}
}

func (e Encoder) Encode(op Op) error {
	_, err := e.w.Write(op.data())
	return err
}

func Parse(data []byte) (Op, error) {
	switch code := op(data).OpCode(); code {
	case OpQuery:
		return parseQueryOp(data)
	case OpReply:
		return ParseReplyOp(data)
	case OpMsg:
		return ParseMsgOp(data)
	default:
		return nil, fmt.Errorf("unknown op code %v", code)
	}

}

func parseCString(data []byte) (string, error) {
	// TODO(ajwerner): use unsafe to construct a string from the bytes
	endIdx := bytes.IndexByte(data, 0)
	if endIdx == -1 {
		return "", fmt.Errorf("invalid message data does not contain a NULL byte")
	}
	return string(data[:endIdx]), nil
}

// Document is a raw, unparsed BSON document.
type Document []byte

func NewDocument(v interface{}) (Document, error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Document(data), nil
}

func (d Document) Unmarshal(into interface{}) error {
	return bson.Unmarshal([]byte(d), into)
}

func (d Document) String() string {
	var v map[string]interface{}
	if err := d.Unmarshal(&v); err != nil {
		return err.Error()
	}
	d, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	return string(d)
}

func parseDocument(data []byte) (Document, error) {
	l := binary.LittleEndian.Uint32(data)
	if int(l) > len(data) {
		return nil, fmt.Errorf("data too short, got %d, expected %d", len(data), int(l))
	}
	return Document(data[:l]), nil
}

////////////////////////////////////////////////////////////////////////////////
// Op
////////////////////////////////////////////////////////////////////////////////

/*
struct MsgHeader {
    int32   messageLength; // total message size, including this
    int32   requestID;     // identifier for this message
    int32   responseTo;    // requestID from the original request
                           //   (used in responses from db)
    int32   opCode;        // request type - see table below for details
}
*/

const headerSize = 4 * 4

type op []byte

func (m op) String() string {
	return fmt.Sprintf("[%v %d %d %d]",
		m.OpCode(), m.MessageLength(), m.RequestID(), m.ResponseTo())
}

var le = binary.LittleEndian

func (h op) MessageLength() uint32 { return le.Uint32(h[0:4]) }
func (h op) RequestID() uint32     { return le.Uint32(h[4:8]) }
func (h op) ResponseTo() uint32    { return le.Uint32(h[8:12]) }
func (h op) OpCode() OpCode        { return OpCode(le.Uint32(h[12:16])) }
func (h op) data() []byte          { return []byte(h) }
func (h op) setSize()              { le.PutUint32(h, uint32(len(h))) }

////////////////////////////////////////////////////////////////////////////////
// QueryOp
////////////////////////////////////////////////////////////////////////////////

/*
struct OP_QUERY {
    MsgHeader header;                 // standard message header
    int32     flags;                  // bit vector of query options.  See below for details.
    cstring   fullCollectionName ;    // "dbname.collectionname"
    int32     numberToSkip;           // number of documents to skip
    int32     numberToReturn;         // number of documents to return
                                      //  in the first OP_REPLY batch
    document  query;                  // query object.  See below for details.
  [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
                                      //  to return.  See below for details.
}
*/

type QueryOp struct {
	op
	Flags                uint32
	FullCollection       string
	NumberToSkip         uint32
	NumberToReturn       uint32
	Query                Document
	ReturnFieldsSelector Document
}

const flagsOffset = headerSize
const collectionOffset = flagsOffset + 4

func NewQueryOp(
	requestID, responseTo, flags uint32,
	collection string,
	numberToSkip, numberToReturn uint32,
	query, fields interface{},
) (*QueryOp, error) {
	q, err := encodeQueryOp(requestID, responseTo, flags, collection,
		numberToSkip, numberToReturn, query, fields)
	if err != nil {
		return nil, err
	}
	return parseQueryOp(q)
}

func encodeQueryOp(
	requestID, responseTo, flags uint32,
	collection string,
	numberToSkip, numberToReturn uint32,
	query, fields interface{},
) ([]byte, error) {
	var buf bytes.Buffer
	encodeHeader(&buf, 0, requestID, responseTo, OpQuery)
	binary.Write(&buf, le, flags)
	binary.Write(&buf, le, numberToSkip)
	binary.Write(&buf, le, numberToReturn)
	queryData, err := bson.Marshal(query)
	if err != nil {
		return nil, err
	}
	buf.Write(queryData)
	if fields != nil {
		fieldsData, err := bson.Marshal(fields)
		if err != nil {
			return nil, err
		}
		buf.Write(fieldsData)
	}
	ret := buf.Bytes()
	op(ret).setSize()
	return ret, nil
}

func parseQueryOp(data []byte) (*QueryOp, error) {
	collection, err := parseCString(data[collectionOffset:])
	if err != nil {
		return nil, err
	}
	toSkipOffset := collectionOffset + len(collection) + 1
	toReturnOffset := toSkipOffset + 4
	queryOffset := toReturnOffset + 4
	query, err := parseDocument(data[queryOffset:])
	if err != nil {
		return nil, err
	}
	var fields Document
	if fieldsOffset := queryOffset + len(query); fieldsOffset < len(data) {
		if fields, err = parseDocument(data[fieldsOffset:]); err != nil {
			return nil, err
		}
	}
	return &QueryOp{
		op:                   data,
		Flags:                le.Uint32(data[headerSize:]),
		FullCollection:       collection,
		NumberToSkip:         le.Uint32(data[toSkipOffset:]),
		NumberToReturn:       le.Uint32(data[toReturnOffset:]),
		Query:                query,
		ReturnFieldsSelector: fields,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// ReplyOp
////////////////////////////////////////////////////////////////////////////////

/*
struct {
    MsgHeader header;         // standard message header
    int32     responseFlags;  // bit vector - see details below
    int64     cursorID;       // cursor id if client needs to do get more's
    int32     startingFrom;   // where in the cursor this reply is starting
    int32     numberReturned; // number of documents in the reply
    document* documents;      // documents
}
*/

type ReplyOp struct {
	op
	ResponseFlags  ResponseFlags
	CursorID       uint64
	StartingFrom   uint32
	NumberReturned uint32
	Documents      []Document
}

/*
ResponseFlags is a it vector to specify flags. The bit values correspond to the following:

    0 corresponds to CursorNotFound. Is set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
    1 corresponds to QueryFailure. Is set when query failed. Results consist of one document containing an “$err” field describing the failure.
    2 corresponds to ShardConfigStale. Drivers should ignore this. Only mongos will ever see this set, in which case, it needs to update config from the server.
    3 corresponds to AwaitCapable. Is set when the server supports the AwaitData Query option. If it doesn’t, a client should sleep a little between getMore’s of a Tailable cursor. Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.
    4-31 are reserved. Ignore.
*/
type ResponseFlags uint32

const (
	CursorNotFound ResponseFlags = 1 << iota
	QueryFailure
	ShardConfigStale
	AwaitCapable
)

func NewReplyOp(
	requestID, responseTo uint32,
	flags ResponseFlags,
	cursorID uint64,
	startingFrom, numberReturned uint32,
	documents ...Document,
) (*ReplyOp, error) {
	data, err := encodeReplyOp(requestID, responseTo, flags, cursorID, startingFrom, numberReturned, documents...)
	if err != nil {
		return nil, err
	}
	return ParseReplyOp(data)
}

func encodeHeader(w *bytes.Buffer, length, requestID, responseTo uint32, code OpCode) {
	binary.Write(w, le, uint32(0))
	binary.Write(w, le, requestID)
	binary.Write(w, le, responseTo)
	binary.Write(w, le, uint32(code))
}

func encodeReplyOp(
	requestID, responseTo uint32,
	flags ResponseFlags,
	cursorID uint64,
	startingFrom, numberReturned uint32,
	documents ...Document,
) ([]byte, error) {
	var buf bytes.Buffer
	encodeHeader(&buf, 0, requestID, responseTo, OpReply)
	binary.Write(&buf, le, uint32(flags))
	binary.Write(&buf, le, cursorID)
	binary.Write(&buf, le, startingFrom)
	binary.Write(&buf, le, numberReturned)
	for _, d := range documents {
		buf.Write([]byte(d))
	}
	ret := buf.Bytes()
	op(ret).setSize()
	return ret, nil
}

func ParseReplyOp(data []byte) (*ReplyOp, error) {
	const responseFlagsOffset = headerSize
	const cursorOffset = flagsOffset + 4
	const startingFromOffset = cursorOffset + 8
	const numberReturnedOffset = startingFromOffset + 4
	const documentsOffset = numberReturnedOffset + 4
	r := &ReplyOp{
		op:             data,
		ResponseFlags:  ResponseFlags(le.Uint32(data[headerSize:])),
		CursorID:       le.Uint64(data[cursorOffset:]),
		StartingFrom:   le.Uint32(data[startingFromOffset:]),
		NumberReturned: le.Uint32(data[numberReturnedOffset:]),
	}
	offset := documentsOffset
	for offset < len(data) {
		d, err := parseDocument(data[offset:])
		if err != nil {
			return nil, err
		}
		r.Documents = append(r.Documents, d)
		offset += len(d)
	}
	return r, nil
}

////////////////////////////////////////////////////////////////////////////////
// MsgOp
////////////////////////////////////////////////////////////////////////////////

/*
OP_MSG {
    MsgHeader header;          // standard message header
    uint32 flagBits;           // message flags
    Sections[] sections;       // data sections
    optional<uint32> checksum; // optional CRC-32C checksum
}
*/

type MsgOp struct {
	op
	Flags    MsgFlags
	Sections []Section

	// TODO(ajwerner): add checksum
	// TODO(ajwerner): consider pre-allocating an array for the backing storage of sections
}

func ParseMsgOp(data []byte) (*MsgOp, error) {
	const msgFlagsOffset = headerSize
	offset := msgFlagsOffset + 4
	var sections []Section
	for offset < len(data) {
		section, err := parseSection(data[offset:])
		if err != nil {
			return nil, err
		}
		sections = append(sections, section)
		offset += section.Length()
	}
	return &MsgOp{
		op:       data,
		Sections: sections,
	}, nil
}

func encodeMsgOp(
	requestID, responseTo uint32, flags MsgFlags, sections ...Section,
) ([]byte, error) {
	var buf bytes.Buffer
	encodeHeader(&buf, 0, requestID, responseTo, OpMsg)
	binary.Write(&buf, le, uint32(flags))
	for _, s := range sections {
		s.encode(&buf)
	}
	ret := buf.Bytes()
	op(ret).setSize()
	return ret, nil
}

func NewMsgOp(requestID, responseTo uint32, flags MsgFlags, sections ...Section) (*MsgOp, error) {
	d, err := encodeMsgOp(requestID, responseTo, flags, sections...)
	if err != nil {
		return nil, err
	}
	return ParseMsgOp(d)
}

func (m *MsgOp) String() string {
	return fmt.Sprintf("[%v %v]", m.op, m.Sections)
}

func parseSection(data []byte) (s Section, err error) {
	switch s.Kind = SectionKind(data[0]); s.Kind {
	case BodySection:
		s.Document, err = parseDocument(data[1:])
	case DocumentSequenceSection:
		s.DocumentSequence, err = parseDocumentSequence(data[1:])
	default:
		panic(fmt.Errorf("unknown section kind %v", s.Kind))
	}
	return
}

/*
MsgFlag is a bitmask encoding flags that modify the format and behavior of OP_MSG.

The first 16 bits (0-15) are required and parsers MUST error if an unknown bit is set.

The last 16 bits (16-31) are optional, and parsers MUST ignore any unknown set bits. Proxies and other message forwarders MUST clear any unknown optional bits before forwarding messages.
Bit 	Name 	Request 	Response 	Description
0 	checksumPresent 	✓ 	✓ 	The message ends with 4 bytes containing a CRC-32C [1] checksum. See Checksum for details.
1 	moreToCome 	✓ 	✓ 	Another message will follow this one without further action from the receiver. The receiver MUST NOT send another message until receiving one with moreToCome set to 0 as sends may block, causing deadlock. Requests with the moreToCome bit set will not receive a reply. Replies will only have this set in response to requests with the exhaustAllowed bit set.
16 	exhaustAllowed 	✓

The client is prepared for multiple replies to this request using the moreToCome bit. The server will never produce replies with the moreToCome bit set unless the request has this bit set.

This ensures that multiple replies are only sent when the network layer of the requester is prepared for them.
*/
type MsgFlags uint32

const (
	ChecksumPresent MsgFlags = 1 << iota
	MoreToCome
	ExhaustAllowed MsgFlags = 1 << 16
)

type SectionKind byte

const (
	BodySection             SectionKind = 0
	DocumentSequenceSection SectionKind = 1
)

// Length of the section in bytes, including the kind byte
func (s Section) Length() int {
	switch s.Kind {
	case BodySection:
		return 1 + len(s.Document)
	case DocumentSequenceSection:
		return 1 + len(s.DocumentSequence)
	default:
		panic(fmt.Errorf("unknown kind %v", s.Kind))
	}
}

func (s Section) String() string {
	switch s.Kind {
	case BodySection:
		return s.Document.String()
	case DocumentSequenceSection:
		return s.DocumentSequence.String()
	default:
		panic(fmt.Errorf("unknown kind %v", s.Kind))
	}
}

type Section struct {
	Kind SectionKind
	Document
	DocumentSequence
}

func (s Section) encode(w *bytes.Buffer) {
	w.WriteByte(byte(s.Kind))
	if s.Kind == BodySection {
		w.Write([]byte(s.Document))
	} else {
		w.Write([]byte(s.DocumentSequence))
	}
}

type DocumentSequence []byte

func parseDocumentSequence(data []byte) (DocumentSequence, error) {
	l := DocumentSequence(data).Length()
	// TODO(ajwerner): error handling
	return DocumentSequence(data[:int(l)]), nil
}

func (s DocumentSequence) Length() uint32 {
	return le.Uint32([]byte(s[:4]))
}

func (s DocumentSequence) Identifier() string {
	id, err := parseCString([]byte(s[4:]))
	if err != nil {
		panic(err)
	}
	return id
}

type DocumentIterator func(d Document) (wantMore bool)

func (s DocumentSequence) Iterate(f DocumentIterator) error {
	id := s.Identifier()
	data := []byte(s[4+len(id)+1:])
	for len(data) > 0 {
		d, err := parseDocument(data)
		if err != nil {
			return err
		}
		if !f(d) {
			return nil
		}
		data = data[len(d):]
	}
	return nil
}

func (s DocumentSequence) String() string {
	var b strings.Builder
	b.WriteString("{\"")
	b.WriteString(s.Identifier())
	b.WriteString("\":[")
	written := false
	if err := s.Iterate(func(d Document) (wantMore bool) {
		if written {
			b.WriteString(", ")
		}
		b.WriteString(d.String())
		written = true
		return true
	}); err != nil {
		panic(err)
	}
	return b.String()
}
