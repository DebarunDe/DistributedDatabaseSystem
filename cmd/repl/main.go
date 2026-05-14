package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	lock "github.com/your-username/DistributedDatabaseSystem/internal/Lock"
	sqllayer "github.com/your-username/DistributedDatabaseSystem/internal/SQLLayer"
	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

const defaultCacheSize = 256

func openOrCreate(path string) (pagemanager.PageManager, error) {
	var (
		disk pagemanager.PageManager
		err  error
	)
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		disk, err = pagemanager.NewDB(path)
	} else {
		disk, err = pagemanager.OpenDB(path)
	}
	if err != nil {
		return nil, err
	}
	wal, err := pagemanager.NewWAL(disk, path)
	if err != nil {
		_ = disk.Close()
		return nil, err
	}
	return pagemanager.NewBufferPool(wal, defaultCacheSize), nil
}

func formatField(f btree.Field) string {
	switch v := f.Value.(type) {
	case btree.IntValue:
		return fmt.Sprintf("%d", v.V)
	case btree.StringValue:
		return v.V
	case btree.NullValue:
		return "NULL"
	default:
		return "?"
	}
}

func printResults(rs *sqllayer.ResultSet) {
	fmt.Println(strings.Join(rs.Columns, " | "))
	fmt.Println(strings.Repeat("-", max(1, len(strings.Join(rs.Columns, " | ")))))
	for _, row := range rs.Rows {
		parts := make([]string, len(row.Fields))
		for i, f := range row.Fields {
			parts[i] = formatField(f)
		}
		fmt.Println(strings.Join(parts, " | "))
	}
	fmt.Printf("(%d row(s))\n", len(rs.Rows))
}

func main() {
	dbPath := flag.String("db", "", "path to database file (required)")
	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "usage: repl -db <path>")
		os.Exit(1)
	}

	pm, err := openOrCreate(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = pm.Close() }()

	bt := btree.NewBTree(pm)
	sc := sqllayer.NewSchemaCatalog(bt)
	if err := sc.LoadSchemas(); err != nil {
		fmt.Fprintf(os.Stderr, "load schemas: %v\n", err)
		os.Exit(1)
	}
	tm := lock.NewTransactionManager(bt)
	ex := sqllayer.NewExecutor(sc, bt, tm)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("sql> ")
		if !scanner.Scan() {
			fmt.Println()
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit") {
			break
		}

		tokens, err := sqllayer.Tokenize(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}
		stmt, err := sqllayer.Parse(tokens)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}
		txn := tm.Begin()
		rs, err := ex.Execute(stmt, txn.Id)
		if err != nil {
			tm.Rollback(txn.Id)
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}
		tm.Commit(txn.Id)
		if rs != nil {
			printResults(rs)
		} else {
			fmt.Println("OK")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}
}
