// Package server implements the MongoDB wire protocol parsing
//
package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ajwerner/docroach/commands"
	"github.com/ajwerner/docroach/protocol"
	"gopkg.in/mgo.v2/bson"
)

// TODO(ajwerner): reasonable error responses.

type Server struct {
	addr string
	db   *sql.DB
}

func New(addr string, db *sql.DB) *Server {
	return &Server{addr: addr, db: db}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		// handle error
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
			// handle error
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	ctx := context.Background()
	defer conn.Close()
	v := &visitor{conn: conn, s: s}
	opDecoder := protocol.NewDecoder(conn)
	encoder := protocol.NewEncoder(conn)
	for {
		op, err := opDecoder.Decode()
		switch err {
		case nil:
		case io.EOF:
			return
		default:
			panic(err)
		}
		c, err := commands.NewCommand(op)
		if err != nil {
			panic(err)
		}
		resp, err := c.Visit(ctx, v)
		if err != nil {
			panic(err)
		}
		respOp, err := resp.ToOp(op)
		if err != nil {
			panic(err)
		}
		if err := encoder.Encode(respOp); err != nil {
			panic(err)
		}
	}
}

// The first pass on this library is going to use high level abstractions to unmarshal
// and marshall data.

type visitor struct {
	conn net.Conn
	s    *Server
}

func NewConnection(conn net.Conn) commands.Visitor {
	return &visitor{conn: conn}
}

var _ commands.Visitor = (*visitor)(nil)

func (v *visitor) VisitGetLog(
	ctx context.Context, c *commands.GetLog,
) (*commands.GetLogResponse, error) {
	return &commands.GetLogResponse{TotalLinesWritten: 0, Ok: true}, nil
}

func (v *visitor) VisitIsMaster(
	ctx context.Context, c *commands.IsMaster,
) (*commands.IsMasterResponse, error) {
	return &commands.IsMasterResponse{
		IsMaster:                     true,
		MinWireVersion:               0,
		MaxWireVersion:               6,
		Ok:                           true,
		LocalTime:                    int64(time.Now().UTC().UnixNano() / 1000000), // TODO(ajwerner): is this millis?
		ReadOnly:                     false,
		MaxWriteBatch:                10000,
		LogicalSessionTimeoutMinutes: 30,
		MaxBsonObjectSize:            1 << 20,
		MaxMessageSize:               1 << 24,
	}, nil
}

func (v *visitor) VisitBuildInfo(
	ctx context.Context, c *commands.BuildInfo,
) (*commands.BuildInfoResponse, error) {
	return &commands.BuildInfoResponse{
		GitVersion:   "63c3bdb0e580178b304ff40b1bbb79a54c89ebef",
		Version:      "3.6.3",
		VersionArray: [4]uint32{3, 6, 3, 0},
		Bits:         64,
		OpenSsl: commands.OpenSslInfo{
			Compiled: "OpenSSL 1.1.1 11 Sep 2018",
			Running:  "OpenSSL 1.1.1 11 Sep 2018",
		},
		BuildEnv: commands.BuildEnvInfo{
			DistArch: "x86_64",
		},
		MaxBsonObjectSize: 1 << 24,
		Ok:                true,
	}, nil
}

func (v *visitor) VisitWhatsMyUri(
	ctx context.Context, c *commands.WhatsMyUri,
) (*commands.WhatsMyUriResponse, error) {
	return &commands.WhatsMyUriResponse{
		You: v.conn.RemoteAddr().String(),
		Ok:  true,
	}, nil
}

func (v *visitor) VisitReplSetGetStatus(
	ctx context.Context, c *commands.ReplSetGetStatus,
) (*commands.ErrorResponse, error) {
	return commands.NewErrorResponse(commands.NoReplicationEnabled,
		fmt.Errorf("not running with --replSet")), nil
}

func (v *visitor) VisitFind(ctx context.Context, c *commands.Find) (*commands.FindResponse, error) {
	tx, err := v.s.db.BeginTx(ctx, nil)
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
	}()
	if err != nil {
		return nil, err
	}
	q, err := tx.Query("CREATE DATABASE IF NOT EXISTS " + c.DB + "; " +
		"CREATE TABLE IF NOT EXISTS " + c.DB + "." + c.Collection + `(
    objectID STRING PRIMARY KEY,
    body JSONB NOT NULL
); SELECT body FROM ` + c.DB + "." + c.Collection + ";")
	if err != nil {
		return nil, err
	}
	for q.Next() {
		var doc string
		if err := q.Scan(&doc); err != nil {
			return nil, err
		}

	}
	return nil, fmt.Errorf("not implemented")
}

func (v *visitor) VisitInsert(
	ctx context.Context, c *commands.Insert,
) (*commands.InsertResponse, error) {
	tx, err := v.s.db.BeginTx(ctx, nil)
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
	}()
	if err != nil {
		return nil, err
	}
	for _, d := range c.Documents {
		var id string
		switch _id := d["_id"].(type) {
		case bson.ObjectId:
			id = _id.Hex()
		case *bson.ObjectId:
			id = _id.Hex()
		case string:
			id = _id
		default:
			return nil, fmt.Errorf("missing _id")
		}
		js, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + c.DB)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec("CREATE TABLE IF NOT EXISTS " + c.DB + "." + c.Collection + ` (
    objectid STRING PRIMARY KEY,
    body JSONB NOT NULL
); `)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec("UPSERT INTO "+c.DB+"."+c.Collection+" VALUES ($1, $2);", id, js)
		if err != nil {
			return nil, err
		}
	}
	return &commands.InsertResponse{N: len(c.Documents), Ok: true}, nil
}
