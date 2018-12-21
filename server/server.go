// Package server attempts to implement the mongodb database TCP server.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ajwerner/docroach/commands"
	"github.com/ajwerner/docroach/protocol"
	"github.com/golang/glog"
	"github.com/jackc/pgx"
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
		glog.V(2).Infof("sending response: %#v", resp)
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
		"not running with --replSet"), nil
}

type infixValueOperator struct {
	op    string
	field string
	val   interface{}
}

type operator interface {
	writeQuery([]interface{}, io.Writer) (values []interface{})
}

func (o infixValueOperator) writeQuery(values []interface{}, w io.Writer) []interface{} {
	// TODO(ajwerner): Deal with splitting and path semantics.
	fmt.Fprintf(w, "document->'%s'=$%d", o.field, len(values)+1)
	return append(values, o.val)
}

type orOperator struct {
	clauses []operator
}

type andOperator struct {
	cluases []operator
}

func hasNonOpKeys(m map[string]interface{}) bool {
	for f := range m {
		if !strings.HasPrefix("$", f) {
			return true
		}
	}
	return false
}

func parseFilterOp(field string, val interface{}) (operator, error) {
	switch v := val.(type) {
	case map[string]interface{}:
		if hasNonOpKeys(v) {
			// then it's equality all the way down
			// TODO: decompose this entire map into equality operators
		}

		if len(v) == 1 {

		}

	// case []interface{}:
	case string, float64, bool, nil:
		return &infixValueOperator{field: field, op: "=", val: v}, nil
	default:
		fmt.Printf("hi %v %T\n", v, v)
	}
	return nil, nil
}

func (v *visitor) VisitFind(
	ctx context.Context, c *commands.Find,
) (_ *commands.FindResponse, err error) {

	// TODO(ajwerner): need to do projections and filters

	// let's try to do some parsing of this filter
	var filterBuf strings.Builder
	var values []interface{}
	if len(c.Filter) > 0 {
		filterMap := map[string]operator{}

		for field, val := range c.Filter {
			// so there's like a sort of wile set of ways this can go
			// we need to detect whether we're in a value context in which case this is an and
			if strings.HasPrefix(field, "$") {
				// we know that we're inside of a set of predicates.
				return nil, commands.NewErrorResponse(commands.BadValue, "unknown top level operator: %v", field)
			}
			// so now we need to parse the operators
			// if there are any non-$ values in an operator map, then we know this is
			// to be interpretted as a document literal and then we just throw the whole
			// thing into an eq operator
			// also if the type is not a document then we do the same again.
			op, err := parseFilterOp(field, val)
			if err != nil {
				return nil, err
			}
			filterMap[field] = op
		}
		var written bool
		for _, op := range filterMap {
			if op == nil {
				continue
			}
			if !written {
				filterBuf.WriteString(" WHERE ")
			} else {
				filterBuf.WriteString(" AND ")
			}
			values = op.writeQuery(values, &filterBuf)
			written = true
		}
	}

	var tx *pgx.Tx
	tx, err = v.db.Begin()
	if err != nil {
		return nil, err
	}
	ns := c.DB + "." + c.Collection
	q, err := tx.Query("SELECT document FROM "+ns+filterBuf.String(), values...)
	if err != nil {
		tx.Rollback()
		if pgerr, ok := err.(pgx.PgError); ok && pgerr.Routine == "NewUndefinedRelationError" {
			return &commands.FindResponse{
				Ok: true,
				Cursor: commands.FindCursor{
					NS: ns,
				},
			}, nil
		}
		return nil, err
	}
	// TODO(ajwerner): make parsing more efficient by building up a bson.D on the fly.
	var results []interface{}
	for q.Next() {
		var doc map[string]interface{}
		if err := q.Scan(&doc); err != nil {
			return nil, err
		}
		results = append(results, doc)
	}
	return &commands.FindResponse{
		Ok: true,
		Cursor: commands.FindCursor{
			NS:         ns,
			FirstBatch: results,
		},
	}, tx.Commit()
}

func (v *visitor) VisitUpdate(
	ctx context.Context, c *commands.Update,
) (*commands.UpdateResponse, error) {
	return &commands.UpdateResponse{}, nil
}

func commitOrRollback(tx *pgx.Tx, err *error) {
	glog.Infof("in commitOrRollback: %v", *err)
	if *err == nil {
		*err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
}

func createDatabaseCollection(
	ctx context.Context, database, collection string, db *pgx.Conn,
) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// TODO(ajwerner): Reconsider the IF NOT EXISTS here and below.
	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	if err != nil {
		return err
	}
	_, err = tx.Exec("CREATE TABLE IF NOT EXISTS " + database + "." + collection + ` (
      document JSONB,
      id STRING AS (document->>'_id') STORED,
      PRIMARY KEY (id),
      CHECK (length(id) > 0)
	);`)
	if err != nil {
		return err
	}
	return nil
}

func visitInsert(
	ctx context.Context, c *commands.Insert, db *pgx.Conn,
) (*commands.InsertResponse, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	for _, d := range c.Documents {
		js, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec("UPSERT INTO "+c.DB+"."+c.Collection+" VALUES ($1);", js)
		if err != nil {
			tx.Rollback()
			if pgerr, ok := err.(pgx.PgError); ok {
				if pgerr.Routine == "NewUndefinedRelationError" {
					glog.Infof("here", pgerr)
					if err := createDatabaseCollection(ctx, c.DB, c.Collection, db); err != nil {
						return nil, err
					}
					return visitInsert(ctx, c, db)
				} else {
					fmt.Println("wtf ", err)
				}
			}
			return nil, fmt.Errorf("%v %T ", err, err)
		}
		err = tx.Commit()
	}
	return &commands.InsertResponse{N: len(c.Documents), Ok: true}, nil
}

func (v *visitor) VisitInsert(
	ctx context.Context, c *commands.Insert,
) (*commands.InsertResponse, error) {
	if c.DB == "" {
		return nil, fmt.Errorf("insert: missing db")
	}
	return visitInsert(ctx, c, v.db)
}
