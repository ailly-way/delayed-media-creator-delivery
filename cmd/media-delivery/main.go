package main

import (
	"log"
	"net/http"
	"os"

	mediadelivery "example.com/delayed-media-delivery"
)

func main() {
	key := requireEnv("INFRAI_API_KEY")
	publicURL := requireEnv("PUBLIC_BASE_URL")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	client := mediadelivery.NewInfraiClient(baseURL, key, nil)
	service := mediadelivery.NewService(client, publicURL, nil)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("media delivery worker listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, service.Routes()))
}

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
