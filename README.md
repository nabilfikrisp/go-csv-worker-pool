# Go CSV Worker Pool Learning Project

This repository is a **personal learning project** to understand how to efficiently process large CSV files in Go, focusing on concurrency patterns (worker pools) and database insertion performance.

The main reference and learning material for this project comes from:

- [https://dasarpemrogramangolang.novalagung.com/D-insert-1mil-csv-record-into-db-in-a-minute.html](https://dasarpemrogramangolang.novalagung.com/D-insert-1mil-csv-record-into-db-in-a-minute.html)

The dataset used in this project is sourced from:

- [https://github.com/kyle-w-brown/majestic_million](https://github.com/kyle-w-brown/majestic_million)

This project is **not intended for production use** or as a reusable library.

---

## What this project explores

- Reading large CSV files in Go
- Worker pool patterns using goroutines and channels
- Batch inserts vs optimized bulk inserts
- Comparing standard inserts with PostgreSQL `COPY`
- Basic database migration setup

---

## Requirements

- Go (recent version recommended)
- PostgreSQL
- Git

---

## Setup Instructions

### 0. Environment configuration

Create a `.env` file in the project root with the following content:

```env
POSTGRES_URI=postgres://user:password@localhost:5433/go-csv?sslmode=disable
```

Adjust the values according to your local PostgreSQL setup.

---

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd <repo-name>
```

### 2. Download the dataset

Download `majestic_million.csv` from:

[https://github.com/kyle-w-brown/majestic_million](https://github.com/kyle-w-brown/majestic_million)

Place the file under the following directory:

```text
csv/majestic_million.csv
```

> The CSV file is intentionally ignored by Git.

---

### 3. Run database migrations

If the table already contains data (rows from a previous run), you may want to reset it first:

```bash
go run ./cmd/migrate-down/main.go
```

Then apply the migrations:

Before running any CSV processing, apply the database migrations:

```bash
go run ./cmd/migrate-up/main.go
```

Make sure your database configuration is set correctly (e.g. via environment variables).

---

## Running the examples

This repository contains multiple approaches for processing the CSV file.

### 1. Basic CSV reader with worker pool

```bash
go run ./app/csv-reader-tutorial
```

This version follows the tutorial-style implementation and focuses on understanding the worker pool mechanics.

---

### 2. CSV reader using PostgreSQL COPY

```bash
go run ./app/csv-reader-with-postgres-copy
```

This version uses PostgreSQL's `COPY` mechanism to achieve significantly faster bulk inserts and is intended for performance comparison.

---

## Performance Results

The following results are based on processing **1,000,000 rows** from the Majestic Million dataset. Screenshots of each run are provided to document the measured execution time.

Execution time is measured directly in code using `time.Now()` and `time.Since()`.

On my local machine, using the same dataset and database configuration, I observed the following approximate results:

- **PostgreSQL COPY approach**: ~12 seconds (1,000,000 rows)
- **Simple worker pool approach**: ~69 seconds (1,000,000 rows)

These results highlight the significant performance difference between row-based inserts via a worker pool and PostgreSQL’s native bulk insert mechanism.

> Results are hardware- and configuration-dependent. Screenshots of each run (time taken) are included in this repository for reference.

### Screenshots

Below are the screenshots showing execution time for each approach:

#### PostgreSQL COPY

![assets/pg-copy.png](assets/pg-copy.png)

#### Simple Worker Pool

![assets/worker-pool.png](assets/worker-pool.png)

---

## Notes

- Code structure and decisions prioritize **learning clarity**, not abstraction or API stability.
- Error handling and configuration are intentionally minimal.
- Performance results may vary depending on hardware and database configuration.

---

## License

This project is licensed under the MIT License.
