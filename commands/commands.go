package commands

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ajwerner/docroach/protocol"
	"gopkg.in/mgo.v2/bson"
)

////////////////////////////////////////////////////////////////////////////////
// Interfaces
////////////////////////////////////////////////////////////////////////////////

type Visitor interface {
	VisitWhatsMyUri(context.Context, *WhatsMyUri) (*WhatsMyUriResponse, error)
	VisitBuildInfo(context.Context, *BuildInfo) (*BuildInfoResponse, error)
	VisitGetLog(context.Context, *GetLog) (*GetLogResponse, error)
	VisitIsMaster(context.Context, *IsMaster) (*IsMasterResponse, error)
	VisitReplSetGetStatus(context.Context, *ReplSetGetStatus) (*ErrorResponse, error)
	VisitFind(context.Context, *Find) (*FindResponse, error)
	VisitInsert(context.Context, *Insert) (*InsertResponse, error)
}

var commandNameToObject = map[string]func() Command{
	"isMaster":         func() Command { return new(IsMaster) },
	"whatsmyuri":       func() Command { return new(WhatsMyUri) },
	"buildinfo":        func() Command { return new(BuildInfo) },
	"buildInfo":        func() Command { return new(BuildInfo) },
	"getLog":           func() Command { return new(GetLog) },
	"replSetGetStatus": func() Command { return new(ReplSetGetStatus) },
	"find":             func() Command { return new(Find) },
	"insert":           func() Command { return new(Insert) },
}

func NewCommand(op protocol.Op) (Command, error) {
	switch op := op.(type) {
	case *protocol.MsgOp:
		// TODO: deal with document stream sections better
		var bodySection *protocol.Section
		var documentsSection *protocol.Section
		for i, s := range op.Sections {
			if s.Kind == protocol.BodySection {
				bodySection = &op.Sections[i]
			} else {
				documentsSection = &op.Sections[i]
			}
		}
		if bodySection == nil {
			return nil, fmt.Errorf("failed to find a body section")
		}
		c, err := documentToCommand(bodySection.Document)
		if err != nil {
			return nil, err
		}
		if insert, ok := c.(*Insert); ok {
			if documentsSection != nil {
				documentsSection.Iterate(func(d protocol.Document) bool {
					var m map[string]interface{}
					if err = d.Unmarshal(&m); err != nil {
						return false
					}
					insert.Documents = append(insert.Documents, m)
					return true
				})
			}
		}
		if err != nil {
			return nil, err
		}
		return c, nil
	case *protocol.QueryOp:
		return documentToCommand(op.Query)
	default:
		return nil, fmt.Errorf("invalid op type %T", op)
	}
}

func documentToCommand(d protocol.Document) (Command, error) {
	var raw bson.RawD
	if err := bson.Unmarshal(d, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("malformed command")
	}
	cf, ok := commandNameToObject[raw[0].Name]
	if !ok {
		return nil, fmt.Errorf("invalid command name %v", raw[0].Name)
	}
	c := cf()
	if err := bson.Unmarshal(d, c); err != nil {
		return nil, err
	}
	return c, nil
}

type Command interface {
	Visit(context.Context, Visitor) (Response, error)
}

type Response interface {
	ToOp(from protocol.Op) (protocol.Op, error)
}

type GlobalOptions struct {
	DB string `json:"$db"`
}

////////////////////////////////////////////////////////////////////////////////
// WhatsMyUri
////////////////////////////////////////////////////////////////////////////////

type WhatsMyUri struct {
	GlobalOptions `bson:",inline"`
	WhatsMyUri    bool `bson:"whatsmyuri" json:"whatsmyuri"`
}

type WhatsMyUriResponse struct {
	You string `json:"you" bson:"you"`
	Ok  bool   `json:"ok" bson:"ok"`
}

var requestID uint32

func getRequestID() uint32 {
	return atomic.AddUint32(&requestID, 1)
}

func (r *WhatsMyUriResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	// needs to take a response, marshal it to bson,
	return simpleToOp(from, r)
}

func simpleToOp(from protocol.Op, val interface{}) (protocol.Op, error) {
	switch from := from.(type) {
	case *protocol.MsgOp:
		return toMsgOp(from, val)
	case *protocol.QueryOp:
		return toReplyOp(from, val)
	default:
		return nil, fmt.Errorf("invalid from type %T", from)
	}
}

func toReplyOp(from protocol.Op, val interface{}) (protocol.Op, error) {
	d, err := protocol.NewDocument(val)
	if err != nil {
		return nil, err
	}
	return protocol.NewReplyOp(getRequestID(), from.RequestID(), 0, 0, 0, 1, d)
}

func toMsgOp(from protocol.Op, val interface{}) (protocol.Op, error) {
	d, err := protocol.NewDocument(val)
	if err != nil {
		return nil, err
	}
	return protocol.NewMsgOp(getRequestID(), from.RequestID(), 0, protocol.Section{
		Kind:     protocol.BodySection,
		Document: d,
	})
}

func (w *WhatsMyUri) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitWhatsMyUri(ctx, w)
}

////////////////////////////////////////////////////////////////////////////////
// BuildInfo
////////////////////////////////////////////////////////////////////////////////

type BuildInfo struct {
	GlobalOptions `bson:",inline"`
}

func (c *BuildInfo) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitBuildInfo(ctx, c)
}

type OpenSslInfo struct {
	Running  string `json:"running" bson:"running"`
	Compiled string `json:"compiled" bson:"compiled"`
}

type BuildEnvInfo struct {
	DistMod  string `json:"distmod" bson:"distmod"`
	DistArch string `json:"distarch" bson:"distarch"`
}

type BuildInfoResponse struct {
	GitVersion        string       `json:"gitVersion,omitEmpty" bson:"gitVersion,omitempty"`
	Version           string       `json:"version" bson:"version"`
	VersionArray      [4]uint32    `json:"versionArray" bson:"versionArray"`
	Bits              uint32       `json:"bits" bson:"bits"`
	Modules           []string     `json:"modules" bson:"modules"`
	Allocator         string       `json:"allocator" bson:"allocator"`
	JavascriptEngine  string       `json:"javascriptEngine" bson:"javascriptEngine"`
	OpenSsl           OpenSslInfo  `json:"openssl" bson:"openssl"`
	BuildEnv          BuildEnvInfo `json:"buildEnvironment" bson:"buildEnvironment"`
	Debug             bool         `json:"debug" bson:"debug"`
	MaxBsonObjectSize uint32       `json:"maxBsonObjectSize" bson:"maxBsonObjectSize"`
	Ok                bool         `json:"ok" bson:"ok"`
}

func (r *BuildInfoResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}

////////////////////////////////////////////////////////////////////////////////
// GetLog
////////////////////////////////////////////////////////////////////////////////

type GetLog struct {
	GlobalOptions `bson:",inline"`
	GetLog        string `json:"getLog" bson:"getLog"`
}

func (c *GetLog) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitGetLog(ctx, c)
}

type GetLogResponse struct {
	TotalLinesWritten int      `json:"totalLinesWritten" bson:"totalLinesWritten"`
	Log               []string `json:"log" bson:"log"`
	Ok                bool
}

func (r *GetLogResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}

////////////////////////////////////////////////////////////////////////////////
// IsMaster
////////////////////////////////////////////////////////////////////////////////

type IsMaster struct {
	DB string `json:"$db" bson:"$db"`
}

func (c *IsMaster) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitIsMaster(ctx, c)
}

type IsMasterResponse struct {
	IsMaster                     bool   `json:"isMaster" bson:"isMaster"`
	MinWireVersion               uint32 `json:"minWireVersion" bson:"minWireVersion"`
	MaxWireVersion               uint32 `json:"maxWireVersion" bson:"maxWireVersion"`
	LocalTime                    int64  `json:"localTime" bson:"localTime"`
	ReadOnly                     bool   `json:"readOnly" bson:"readOnly"`
	MaxBsonObjectSize            uint32 `json:"maxBsonObjectSize" bson:"maxBsonObjectSize"`
	MaxMessageSize               uint32 `json:"maxMessageSize" bson:"maxMessageSize"`
	LogicalSessionTimeoutMinutes uint32 `json:"logicalSessionTimeoutMinutes" bson:"logicalSessionTimeoutMinutes"`
	MaxWriteBatch                uint32 `json:"maxWriteBatch" bson:"maxWriteBatch"`
	Ok                           bool   `json:"ok" bson:"ok"`
}

func (r *IsMasterResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}

////////////////////////////////////////////////////////////////////////////////
// ReplSetGetStatus
////////////////////////////////////////////////////////////////////////////////

type ReplSetGetStatus struct {
	GlobalOptions
}

func (c *ReplSetGetStatus) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitReplSetGetStatus(ctx, c)
}

////////////////////////////////////////////////////////////////////////////////
// Insert
////////////////////////////////////////////////////////////////////////////////

type Insert struct {
	GlobalOptions `bson:",inline"`
	DB            string                   `json:"$db" bson:"$db"`
	Collection    string                   `json:"insert" bson:"insert"`
	Documents     []map[string]interface{} `json:"documents" bson:"documents"`
}

func (c *Insert) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitInsert(ctx, c)
}

type InsertResponse struct {
	N  int  `json:"n" bson:"n"`
	Ok bool `json:"ok" bson:"ok"`
}

func (r *InsertResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}

////////////////////////////////////////////////////////////////////////////////
// Find
////////////////////////////////////////////////////////////////////////////////

type Find struct {
	DB         string `json:"$db" bson:"$db"`
	Collection string `json:"find" bson:"find"`
}

func (c *Find) Visit(ctx context.Context, v Visitor) (Response, error) {
	return v.VisitFind(ctx, c)
}

type Cursor struct {
	NS         string        `json:"ns" bson:"ns"`
	Id         uint64        `json:"id" bson:"id"`
	FirstBatch []interface{} `json:"firstBatch,omitempty" bson:"firstBatch,omitempty"`
	NextBatch  []interface{} `json:"nextBatch,omitempty" bson:"nextBatch,omitempty"`
}

type FindResponse struct {
	Cursor Cursor `json:"cursor" bson:"cursor"`
	Ok     bool   `json:"ok" bson:"ok"`
}

func (r *FindResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}

////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////

//go:generate stringer --type ErrorCode

type ErrorCode int

// ErrorCodes are being added as needed.
const (
	NoReplicationEnabled ErrorCode = 76
)

type ErrorResponse struct {
	Ok       bool      `json:"ok" bson:"ok"`
	ErrMsg   string    `json:"errmsg" bson:"errmsg"`
	Code     ErrorCode `json:"code" bson:"code"`
	CodeName string    `json:"codeName" bson:"codeName"`
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("[%v %v] %v", r.CodeName, r.Code, r.ErrMsg)
}

func NewErrorResponse(code ErrorCode, err error) *ErrorResponse {
	return &ErrorResponse{
		Code:     code,
		CodeName: code.String(),
		ErrMsg:   err.Error(),
	}
}

func (r *ErrorResponse) ToOp(from protocol.Op) (protocol.Op, error) {
	return simpleToOp(from, r)
}
