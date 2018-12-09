package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/ajwerner/docroach/server"
	_ "github.com/lib/pq"
)

func main() {
	pgurl := flag.String("pgurl", "postgresql://root@localhost:26257/?sslmode=disable", "url to database")
	listenAddr := flag.String("addr", ":27017", "address to listen on")
	db, err := sql.Open("postgres", *pgurl)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	fmt.Printf("Listening on %v", *listenAddr)
	s := server.New(*listenAddr, db)
	log.Fatal("failed to run server: ", s.ListenAndServe())
}
