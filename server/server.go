// Package server attempts to implement the mongodb database TCP server.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ajwerner/docroach/commands"
	"github.com/ajwerner/docroach/protocol"
	"github.com/golang/glog"
	"github.com/jackc/pgx"
	"gopkg.in/mgo.v2/bson"
)

// TODO(ajwerner): lazily create missing collection tables so we don't have to do multiple transactions
// TODO(ajwerner): use a computed column for the primary key
// TODO(ajwerner): reasonable error responses.
// TODO(ajwerner): make comments more serious and professional :p

// Server attempts to implement a mongo server.
type Server struct {
	addr          string
	dbConnections *pgx.ConnPool
}

func New(addr string, dbConnections *pgx.ConnPool) *Server {
	return &Server{
		addr:          addr,
		dbConnections: dbConnections,
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	glog.Infof("client connected %v", conn.RemoteAddr())
	ctx := context.Background()
	defer conn.Close()
	defer glog.Infof("client disconnected %v", conn.RemoteAddr())
	db, err := s.dbConnections.Acquire()
	if err != nil {
		// TODO(ajwerner): Tell the client about the error (something internal?).
		return
	}
	defer s.dbConnections.Release(db)
	v := newVisitor(conn.RemoteAddr(), db)
	opDecoder := protocol.NewDecoder(conn)
	encoder := protocol.NewEncoder(conn)
	for {
		op, err := opDecoder.Decode()
		switch err {
		case nil:
		case io.EOF:
			return
		default:
			glog.Infof("error decoding client op: %v", err)
			return
		}
		c, err := commands.NewCommand(op)
		if err != nil {
			glog.Errorf("failed to parse command from op: %v", err)
			return
		}
		// TODO(ajwerner): Give requests some sort of ID.
		// Maybe opentracing and context and even cockroach/util/log and its
		// contextual logging.
		glog.V(2).Infof("received command: %#v", c)
		resp, err := c.Visit(ctx, v)
		if err != nil {
			glog.Errorf("failed to visit command %#v: %v", c, err)
			return
		}
		glog.V(2).Infof("sending response: %#v, err", c, err)
		respOp, err := resp.ToOp(op)
		if err != nil {
			glog.Errorf("failed to encode response: %v", err)
			return
		}
		if err := encoder.Encode(respOp); err != nil {
			glog.Errorf("failed to write op: %v", err)
			return
		}
	}
}

type visitor struct {
	remoteAddr net.Addr
	db         *pgx.Conn
}

func newVisitor(remoteAddr net.Addr, db *pgx.Conn) *visitor {
	return &visitor{
		remoteAddr: remoteAddr,
		db:         db,
	}
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

func (v *visitor) VisitDelete(
	ctx context.Context, c *commands.Delete,
) (*commands.DeleteResponse, error) {
	// TODO(ajwerner): this
	return &commands.DeleteResponse{
		N:  0,
		Ok: true,
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
		You: v.remoteAddr.String(),
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
	tx, err := v.db.Begin()
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
	tx, err := v.db.Begin()
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
