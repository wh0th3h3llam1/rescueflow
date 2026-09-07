package main

import (
	"log"
	"net/http"
	"os"

	"github.com/rescueflow/rescueflow/internal/api"
	"github.com/rescueflow/rescueflow/internal/platform"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	workflow := platform.NewWorkflow()
	server := api.New(workflow)
	log.Printf(`{"severity":"INFO","service":"gateway-service","message":"RescueFlow listening","address":%q}`, addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
