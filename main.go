package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/pi"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mattermost/morph/models"
)

const (
	queryTimeout     = time.Hour
	statQueryTimeout = 30 * time.Second
)

func main() {
	var startingMigration int
	var sourceDir, dsn, awsAccessKey, awsSecretKey, awsRegion string
	flag.IntVar(&startingMigration, "start", 0, "Starting migration number from where to apply migrations")
	flag.StringVar(&sourceDir, "source_dir", "", "Source directory containing the migrations. Example: ~/mattermost-server/server/channels/db/migrations/postgres")
	flag.StringVar(&dsn, "dsn", "postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable", "Database DSN to connect to")
	flag.StringVar(&awsAccessKey, "access_key", "", "AWS Access Key")
	flag.StringVar(&awsSecretKey, "secret_key", "", "AWS Secret Key")
	flag.StringVar(&awsRegion, "region", "", "AWS region")
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

	creds := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: creds,
	})
	if err != nil {
		logger.Printf("Error initializing AWS session: %v\n", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), statQueryTimeout)
	defer cancel()
	_, err = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
	if err != nil {
		logger.Println(err)
		return
	}

	start := time.Now()
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

		resetCtx, resetCancel := context.WithTimeout(context.Background(), statQueryTimeout)
		// Not using defer cancel() because this is running in a loop,
		// and defer won't be called until the cmd exits.
		_, err = pool.Exec(resetCtx, "SELECT pg_stat_statements_reset();")
		if err != nil {
			resetCancel()
			logger.Println(err)
			return
		}
		resetCancel()

		fmt.Println(strings.Repeat("-", 100))
		fmt.Printf("Starting migration: %s\n", entry.Name())

		buf, err := os.ReadFile(path.Join(sourceDir, entry.Name()))
		if err != nil {
			logger.Println(err)
			return
		}

		strBuf := string(buf)
		err = analyzeQuery(strBuf)
		// We don't want to skip running the query if it failed analysis.
		if err != nil {
			logger.Println(err)
		}

		quit := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go checkLocks(&wg, quit, logger, pool)

		execCtx, execCancel := context.WithTimeout(context.Background(), queryTimeout)
		_, err = pool.Exec(execCtx, strBuf)
		if err != nil {
			logger.Println(err)
			execCancel()
			continue
		}
		execCancel()

		// cancel locks query
		close(quit)
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), statQueryTimeout)
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
	end := time.Now()
	fmt.Println(strings.Repeat("-", 100))
	printPerfMetrics(end, start, sess, logger)
}

// TODO: refactor this to a method.
func checkLocks(wg *sync.WaitGroup, quit chan struct{}, logger *log.Logger, pool *pgxpool.Pool) {
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
}

const (
	metricCPUUtil    = "os.cpuUtilization.total.max"
	metricReadKBPS   = "os.diskIO.rdstemp.readKbPS.max"
	metricWriteKBPS  = "os.diskIO.rdstemp.writeKbPS.max"
	metricActiveMem  = "os.memory.active.max"
	metricBuffersMem = "os.memory.buffers.max"
)

// TODO: covert these into methods with logger, sess as a struct field.
func printPerfMetrics(end, start time.Time, sess *session.Session, logger *log.Logger) {
	if end.Sub(start) <= time.Minute {
		fmt.Println("Total runtime was less than a minute. Not capturing any DB metrics.")
		return
	}

	rdsSvc := rds.New(sess)
	instances, err := rdsSvc.DescribeDBInstances(&rds.DescribeDBInstancesInput{})
	if err != nil {
		logger.Println(err)
		return
	}
	// TODO: pagination
	fmt.Println("Choose the DB instance to display metrics from:")
	instanceMap := make(map[int]string)
	for i, inst := range instances.DBInstances {
		instanceMap[i] = *inst.DbiResourceId
		fmt.Printf("[%d] Cluster Name: %s, Instance Name: %s, Endpoint: %s\n", i, *inst.DBClusterIdentifier, *inst.DBInstanceIdentifier, *inst.Endpoint.Address)
	}
	var iNum int
	fmt.Print("Enter the number: ")
	fmt.Scanln(&iNum)

	perfI := pi.New(sess)
	fmt.Println("METRICS:")
	out, err := perfI.GetResourceMetrics(&pi.GetResourceMetricsInput{
		StartTime:       &start,
		EndTime:         &end,
		ServiceType:     aws.String(pi.ServiceTypeRds),
		Identifier:      aws.String(instanceMap[iNum]),
		PeriodInSeconds: aws.Int64(60),
		MetricQueries: []*pi.MetricQuery{
			{
				Metric: aws.String(metricCPUUtil),
			},
			{
				Metric: aws.String(metricReadKBPS),
			},
			{
				Metric: aws.String(metricWriteKBPS),
			},
			{
				Metric: aws.String(metricActiveMem),
			},
			{
				Metric: aws.String(metricBuffersMem),
			},
		},
	})
	if err != nil {
		logger.Println(err)
		return
	}
	// TODO: pagination
	// This part of the code just re-arranges the output in a way that
	// can be printed chronologically. The current output returns a list
	// of metric results, where each array item contains all the data points.
	// We change that to a map keyed by the timestamp, and the value of which
	// is another map which contains the metric name and the measurement for that
	// timestamp.
	metricStore := make(map[int64]map[string]float64)
	for _, metric := range out.MetricList {
		name := *metric.Key.Metric
		for _, point := range metric.DataPoints {
			ts := point.Timestamp.Unix()
			if metricStore[ts] == nil {
				metricStore[ts] = make(map[string]float64)
			}
			metricStore[ts][name] = *point.Value
		}
	}
	tsSlice := make([]int64, 0, len(metricStore))
	for ts := range metricStore {
		tsSlice = append(tsSlice, ts)
	}
	sort.Slice(tsSlice, func(i, j int) bool { return tsSlice[i] < tsSlice[j] })

	// ts					metric metric metric
	// 2023-03-30 13:37:00	val		val		val
	fmt.Printf("%25s %25s %25s %25s %25s %25s\n", "Time", metricCPUUtil, metricReadKBPS, metricWriteKBPS, metricActiveMem, metricBuffersMem)
	for _, ts := range tsSlice {
		fromUnixT := time.Unix(ts, 0)
		metricMap := metricStore[ts]
		fmt.Printf("%25v %25f %25f %25f %25f %25f\n", fromUnixT, metricMap[metricCPUUtil], metricMap[metricReadKBPS], metricMap[metricWriteKBPS], metricMap[metricActiveMem], metricMap[metricBuffersMem])
	}
}
