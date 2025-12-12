package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const dbMaxIdleConns = 4
const dbMaxConns = 100
const totalWorker = 100
const csvFile = "./csv/majestic_million.csv"

var dataHeaders = make([]string, 0)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env: %v", err)
	}

	// Read POSTGRES_URI
	dbURL := os.Getenv("POSTGRES_URI")
	if dbURL == "" {
		log.Fatal("POSTGRES_URI is not set")
	}

	start := time.Now()
	db, err := connectDB(dbURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []any)
	wg := new(sync.WaitGroup)

	go dispatchWorker(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}

func connectDB(url string) (*pgxpool.Pool, error) {
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = dbMaxConns
	cfg.MinConns = dbMaxIdleConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	return pool, nil
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

func dispatchWorker(pool *pgxpool.Pool, jobs <-chan []any, wg *sync.WaitGroup) {
	for workerIndex := range totalWorker {
		go func(workerIndex int, pool *pgxpool.Pool, jobs <-chan []any) {
			counter := 0
			for job := range jobs {
				doTheJob(workerIndex, counter, pool, job)
				wg.Done()
				counter++
			}
		}(workerIndex, pool, jobs)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []any, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// FIRST ROW: MAP CSV HEADERS TO SQL COLUMNS
		if len(dataHeaders) == 0 {
			parsed := make([]string, 0, len(row))
			for _, h := range row {
				key := normalize(h)
				col, ok := headerToColumn[key]
				if !ok {
					log.Fatalf("unknown CSV header: %s (normalized: %s)", h, key)
				}
				parsed = append(parsed, col)
			}

			dataHeaders = parsed
			continue
		}

		// DATA ROWS
		rowOrdered := make([]any, len(row))
		for i, each := range row {
			rowOrdered[i] = each
		}

		wg.Add(1)
		jobs <- rowOrdered
	}

	close(jobs)
}

func doTheJob(workerIndex, counter int, pool *pgxpool.Pool, values []any) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()

	query := fmt.Sprintf(
		"INSERT INTO domain_ranking (%s) VALUES (%s)",
		strings.Join(dataHeaders, ","),
		strings.Join(generatePlaceholders(len(dataHeaders)), ","),
	)

	_, err = conn.Exec(context.Background(), query, values...)
	if err != nil {
		log.Fatal(err)
	}

	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "rows")
	}
}

func generatePlaceholders(n int) []string {
	s := make([]string, n)
	for i := 1; i <= n; i++ {
		s[i-1] = fmt.Sprintf("$%d", i)
	}
	return s
}

func normalize(h string) string {
	h = strings.ToLower(h)
	h = strings.TrimSpace(h)
	return h
}

var headerToColumn = map[string]string{
	"globalrank":       "global_rank",
	"global_rank":      "global_rank",
	"tldrank":          "tld_rank",
	"tld_rank":         "tld_rank",
	"domain":           "domain",
	"tld":              "tld",
	"refsubnets":       "ref_subnets",
	"ref_subnets":      "ref_subnets",
	"refips":           "ref_ips",
	"ref_ips":          "ref_ips",
	"idn_domain":       "idn_domain",
	"idndomain":        "idn_domain",
	"idn_tld":          "idn_tld",
	"idntld":           "idn_tld",
	"prevglobalrank":   "prev_global_rank",
	"prev_global_rank": "prev_global_rank",
	"prevtldrank":      "prev_tld_rank",
	"prev_tld_rank":    "prev_tld_rank",
	"prevrefsubnets":   "prev_ref_subnets",
	"prev_ref_subnets": "prev_ref_subnets",
	"prevrefips":       "prev_ref_ips",
	"prev_ref_ips":     "prev_ref_ips",
}
