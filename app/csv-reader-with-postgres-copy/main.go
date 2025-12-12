package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const csvDir = "./csv/majestic_million.csv"

var processed uint64

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env: %v", err)
	}

	dbURL := os.Getenv("POSTGRES_URI")
	if dbURL == "" {
		log.Fatal("POSTGRES_URI is not set")
	}

	start := time.Now()
	db, err := connectDB(dbURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	totalRows, err := countCSVRows(csvDir)
	if err != nil {
		log.Fatal(err)
	}

	csvFile, err := os.Open(csvDir)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)

	header, err := csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	mappedHeader, err := normalizeAndMapHeaders(header, headerToColumn)
	if err != nil {
		log.Fatal(err)
	}

	indexMap, err := buildIndexMap(mappedHeader, dbColumns)
	if err != nil {
		log.Fatal(err)
	}

	rowChan := make(chan []any, 500)

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			rec, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			parsed := convertRecord(rec, indexMap)
			rowChan <- parsed

			atomic.AddUint64(&processed, 1)
		}
		close(rowChan)
	})

	done := make(chan struct{})
	go func() {
		spinner := []rune{'|', '/', '-', '\\'}
		i := 0

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p := atomic.LoadUint64(&processed)
				percent := float64(p) / float64(totalRows) * 100
				fmt.Printf("\r%c Processing... %.2f%% (%d/%d)", spinner[i], percent, p, totalRows)
				i = (i + 1) % len(spinner)

			case <-done:
				return
			}
		}
	}()

	src := &chanSource{ch: rowChan}
	ctx := context.Background()
	count, err := db.CopyFrom(
		ctx,
		pgx.Identifier{"domain_ranking"},
		dbColumns,
		src,
	)
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	wg.Wait()
	close(done)
	fmt.Printf("\r✓ Processing... 100.00%% (%d/%d) at %v\n", count, totalRows, elapsed)

	db.Close()
}

func connectDB(url string) (*pgxpool.Pool, error) {
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	return pool, nil
}

var headerToColumn = map[string]string{
	"globalrank":     "global_rank",
	"tldrank":        "tld_rank",
	"domain":         "domain",
	"tld":            "tld",
	"refsubnets":     "ref_subnets",
	"refips":         "ref_ips",
	"idn_domain":     "idn_domain",
	"idn_tld":        "idn_tld",
	"prevglobalrank": "prev_global_rank",
	"prevtldrank":    "prev_tld_rank",
	"prevrefsubnets": "prev_ref_subnets",
	"prevrefips":     "prev_ref_ips",
}

func normalizeAndMapHeaders(headers []string, mapper map[string]string) ([]string, error) {
	out := make([]string, len(headers))
	for i, h := range headers {
		key := strings.ToLower(h)
		col, ok := mapper[key]
		if !ok {
			return nil, fmt.Errorf("no mapping for header: %s", h)
		}
		out[i] = col
	}

	return out, nil
}

var dbColumns = []string{
	"global_rank",
	"tld_rank",
	"domain",
	"tld",
	"ref_subnets",
	"ref_ips",
	"idn_domain",
	"idn_tld",
	"prev_global_rank",
	"prev_tld_rank",
	"prev_ref_subnets",
	"prev_ref_ips",
}

func buildIndexMap(header []string, dbCols []string) ([]int, error) {
	index := make([]int, len(dbCols))

	for i, dbCol := range dbCols {
		found := -1
		for j, h := range header {
			if h == dbCol {
				found = j
				break
			}
		}
		if found == -1 {
			return nil, fmt.Errorf("db column %s not found in CSV header", dbCol)
		}
		index[i] = found
	}

	return index, nil
}

func convertRecord(rec []string, indexMap []int) []any {
	out := make([]any, len(indexMap))
	for i, srcIdx := range indexMap {
		out[i] = rec[srcIdx]
	}

	return out
}

type chanSource struct {
	ch  <-chan []any
	row []any
	err error
}

func (s *chanSource) Next() bool {
	r, ok := <-s.ch
	if !ok {
		return false
	}
	s.row = r
	return true
}

func (s *chanSource) Values() ([]any, error) {
	return s.row, nil
}

func (s *chanSource) Err() error {
	return s.err
}

func countCSVRows(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	count := -1
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
