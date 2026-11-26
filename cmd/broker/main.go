package main

import (
	"flag"
	"log"
	"net"

	"eventlog/internal/broker"
	"eventlog/internal/commitlog"
)

func main() {
	// flags = command-line options, like Python's argparse
	id := flag.String("id", "1", "broker ID")
	port := flag.String("port", "9001", "TCP port to listen on")
	dataDir := flag.String("data", "data/broker-1", "data directory")
	flag.Parse()

	// flag.String gives a *pointer*, so *id reads the value (same as C++)
	store, err := commitlog.Open(*dataDir) // creates the folder too
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer store.Close()
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("broker %s listening on :%s (data=%s)", *id, *port, *dataDir)
	b := broker.New(*id, store)
	log.Fatal(b.Serve(ln))
}