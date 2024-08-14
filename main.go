package main

import (
	"flag"
	"log"

	"fts.io/engine"
	"fts.io/server"
)

func main() {

	pathToDump := flag.String("path", "<path_to_wikipedia_archive_dump>", "Path to dump file")
	port := flag.String("port", "5000", "The server port")
	dataDir := flag.String("dataDir", "data", "the directory where the index should be saved")

	flag.Parse()

	ftsEngine, err := engine.New(*pathToDump, *dataDir)

	if err != nil {
		log.Fatal("Failed to build index:", err)
	}

	server.Start(ftsEngine, *port)

}
