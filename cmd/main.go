package main

import (
	"log"
	"myredis/db"
	"myredis/server"
)

func main() {
	// Initialize DB with AOF file
	database := db.NewStandaloneDB("appendonly.aof")

	// Load AOF (Persistence)
	database.Load()

	// Initialize Server
	s := server.NewServer(":6399", database)

	// Start
	log.Fatal(s.Start())
}
