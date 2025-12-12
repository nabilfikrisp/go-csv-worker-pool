package main

import (
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env: %v", err)
	}

	// Read POSTGRES_URI
	dbURL := os.Getenv("POSTGRES_URI")

	if dbURL == "" {
		log.Fatal("POSTGRES_URI is not set")
	}

	// Migration instance
	m, err := migrate.New("file://migrations", dbURL)
	if err != nil {
		log.Fatalf("migrate init failed: %v", err)
	}

	// Reset all migrations
	err = m.Down()
	if err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate reset failed: %v", err)
	}
	log.Println("Database reset (all migrations reverted).")

	// Re-apply all migrations
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate up failed: %v", err)
	}

	log.Println("Migrations applied successfully.")
}
