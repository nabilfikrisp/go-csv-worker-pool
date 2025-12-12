package main

import (
	"fmt"
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env: %v", err)
	}

	// Read POSTGRES_URI
	dbURL := os.Getenv("POSTGRES_URI")
	fmt.Println(dbURL)
	if dbURL == "" {
		log.Fatal("POSTGRES_URI is not set")
	}

	// Initialize migration instance
	m, err := migrate.New(
		"file://migrations",
		dbURL,
	)
	if err != nil {
		log.Fatalf("migrate init failed: %v", err)
	}

	// Run migrations (Up = apply all)
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate up failed: %v", err)
	}

	log.Println("Migrations applied successfully")
}
