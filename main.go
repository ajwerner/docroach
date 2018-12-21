package main

import (
	"flag"

	"github.com/ajwerner/docroach/server"
	"github.com/golang/glog"
	"github.com/jackc/pgx"
)

func main() {
	// TODO(ajwerner): Better and more flags.
	pgurl := flag.String("pgurl", "postgresql://root@localhost:26257/?sslmode=disable", "url to database")
	listenAddr := flag.String("addr", ":27017", "address to listen on")
	flag.Parse()

	// TODO(ajwerner): Provide a more flexible/thoughful conn config.
	dbConfig, err := pgx.ParseConnectionString(*pgurl)
	if err != nil {
		glog.Fatalf("error parsing connection string %v: %v", *pgurl, err)
	}
	dbConns, err := pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: dbConfig})
	if err != nil {
		glog.Fatalf("failed to connect to database (%s): %v", *pgurl, err)
	}
	glog.Infof("Listening on %v", *listenAddr)
	s := server.New(*listenAddr, dbConns)
	if err := s.ListenAndServe(); err != nil {
		glog.Fatalf("failed to run server: ", err)
	}
}
