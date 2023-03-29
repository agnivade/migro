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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mattermost/morph/models"
)

const queryTimeout = time.Hour

func main() {
	var startingMigration int
	var sourceDir, dsn string
	flag.IntVar(&startingMigration, "start", 0, "Starting migration number from where to apply migrations")
	flag.StringVar(&sourceDir, "source_dir", "", "Source directory containing the migrations. Example: ~/mattermost-server/server/channels/db/migrations/postgres")
	flag.StringVar(&dsn, "dsn", "postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable", "Database DSN to connect to")
	flag.Parse()

	var confirm string
	fmt.Print("This tool will apply DB migrations to the database which can be destructive in nature. Please ensure you have a backup before proceeding. Continue? (y/n): ")
	fmt.Scanln(&confirm)

	if strings.ToLower(confirm) != "y" {
		return
	}

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

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Println(err)
		return
	}

	poolCfg.MaxConns = 2 // one for running queries, another for checking locks.

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		logger.Println(err)
		return
	}
	defer pool.Close()

	_, err = pool.Exec(context.TODO(), "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
	if err != nil {
		logger.Println(err)
		return
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".down.sql") {
			continue
		}

		matches := models.Regex.FindStringSubmatch(entry.Name())
		if len(matches) <= 1 {
			logger.Printf("Migration %s does not match regex %s\n", entry.Name(), models.Regex.String())
			continue
		}

		seqNum, err := strconv.Atoi(matches[1])
		if err != nil {
			logger.Println(err)
			continue
		}

		if seqNum < startingMigration {
			continue
		}

		_, err = pool.Exec(context.TODO(), "SELECT pg_stat_statements_reset();")
		if err != nil {
			logger.Println(err)
			return
		}

		fmt.Println(strings.Repeat("-", 100))
		fmt.Printf("Starting migration: %s\n", entry.Name())

		buf, err := os.ReadFile(path.Join(sourceDir, entry.Name()))
		if err != nil {
			logger.Println(err)
			return
		}

		quit := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			// Because we are running the query in a loop, we might get the same locked
			// tables over and over again. This is to get the result of only unique tables.
			lockMap := make(map[string]bool)

			defer func() {
				fmt.Println("LOCKS:")
				for k := range lockMap {
					split := strings.Split(k, "-")
					fmt.Printf("LOCK TYPE: %s, RELATION: %s, MODE: %s, GRANTED: %s\n", split[0], split[1], split[2], split[3])
				}
			}()

			for {
				select {
				case <-quit:
					return
				case <-ticker.C:
					rows, err := pool.Query(context.TODO(), `SELECT locktype, relation::regclass, mode, granted
				FROM pg_catalog.pg_locks l
				LEFT JOIN pg_catalog.pg_database db ON db.oid = l.database
				JOIN pg_class c ON l.relation=c.oid
				WHERE (db.datname = current_database() OR db.datname IS NULL) AND c.relkind='r' AND NOT pid = pg_backend_pid() AND locktype='relation';`)
					if err != nil {
						logger.Println(err)
						return
					}
					var lockType, relation, mode string
					var granted bool
					pgx.ForEachRow(rows, []any{&lockType, &relation, &mode, &granted}, func() error {
						lockMap[fmt.Sprintf("%s-%s-%s-%t", lockType, relation, mode, granted)] = true
						return nil
					})
				}
			}
		}()

		_, err = pool.Exec(context.TODO(), string(buf))
		if err != nil {
			logger.Println(err)
			continue
		}

		// cancel locks query
		close(quit)
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		// Not using defer cancel() because this is running in a loop,
		// and defer won't be called until the cmd exits.
		rows, err := pool.Query(ctx, "SELECT query, total_exec_time, rows FROM pg_stat_statements;")
		if err != nil {
			logger.Println(err)
			cancel()
			continue
		}
		var query string
		var execTime float64
		var numRows int
		pgx.ForEachRow(rows, []any{&query, &execTime, &numRows}, func() error {
			// Ignore the reset and locks query
			if query == "SELECT pg_stat_statements_reset()" || strings.HasPrefix(query, "SELECT locktype, relation::regclass") {
				return nil
			}
			fmt.Println("QUERY: ", query)
			fmt.Printf("EXEC TIME: %f ms, ROWS AFFECTED: %d\n", execTime, numRows)
			return nil
		})
		cancel()
	}
}
