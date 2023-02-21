package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	var startingMigration int
	var sourceDir, dsn string
	flag.IntVar(&startingMigration, "start", 0, "Starting migration number from where to apply migrations")
	flag.StringVar(&sourceDir, "source_dir", "", "Source directory containing the migrations. Example: ~/mattermost-server/server/channels/db/migrations/postgres")
	flag.StringVar(&dsn, "dsn", "postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable", "Database DSN to connect to")
	flag.Parse()

	logger := log.New(os.Stderr, "[migro]: ", log.LstdFlags|log.Lshortfile)

	if sourceDir == "" {
		logger.Println("-source_dir not passed. Please use -h to see all the flags")
		return
	}

	if startingMigration == 0 {
		logger.Println("-start not passed. Please use -h to see all the flags")
		return
	}

	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		logger.Println(err)
		return
	}

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		logger.Println(err)
		return
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
	if err != nil {
		logger.Println(err)
		return
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".down.sql") {
			continue
		}

		split := strings.Split(entry.Name(), "_")
		if len(split) < 2 {
			logger.Printf("_ separator not found for this migration %s. Skipping ..\n", entry.Name())
			continue
		}

		seqNum, err := strconv.Atoi(split[0])
		if err != nil {
			logger.Println(err)
			continue
		}

		if seqNum < startingMigration {
			continue
		}

		_, err = conn.Exec(context.Background(), "SELECT pg_stat_statements_reset();")
		if err != nil {
			logger.Println(err)
			return
		}

		fmt.Printf("Starting migration: %100s\n", entry.Name()+strings.Repeat("-", 45))

		buf, err := os.ReadFile(path.Join(sourceDir, entry.Name()))
		if err != nil {
			logger.Println(err)
			return
		}

		_, err = conn.Exec(context.Background(), fmt.Sprintf("%s", buf))
		if err != nil {
			logger.Println(err)
			continue
		}

		rows, err := conn.Query(context.Background(), "SELECT query, total_exec_time, rows FROM pg_stat_statements;")
		if err != nil {
			logger.Println(err)
			continue
		}
		var query string
		var execTime float64
		var numRows int
		pgx.ForEachRow(rows, []any{&query, &execTime, &numRows}, func() error {
			// Ignore the reset query
			if query == "SELECT pg_stat_statements_reset()" {
				return nil
			}
			fmt.Println("QUERY: ", query)
			fmt.Printf("EXEC TIME: %f ms, ROWS AFFECTED: %d\n", execTime, numRows)
			return nil
		})
	}
}
