package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

var (
	backendFlag   = flag.String("backend", "all", "backend to benchmark: pm, bp, or all")
	opsFlag       = flag.Int("ops", 10000, "number of operations per benchmark")
	cacheSizeFlag = flag.Int("cache-size", 256, "buffer pool LRU cache size (pages)")
	modeFlag      = flag.String("mode", "all", "benchmark mode: write, read, rangescan, or all")
	seedFlag      = flag.Int64("seed", 42, "random seed for reproducibility")
)

type result struct {
	backend string
	mode    string
	ops     int
	total   time.Duration
	p50     time.Duration
	p95     time.Duration
	p99     time.Duration
}

func (r result) opsPerSec() float64 {
	if r.total == 0 {
		return 0
	}
	return float64(r.ops) / r.total.Seconds()
}

func setupPageManager(path string) (pagemanager.PageManager, error) {
	return pagemanager.NewDB(path)
}

func setupBufferPool(path string, cacheSize int) (pagemanager.PageManager, error) {
	disk, err := pagemanager.NewDB(path)
	if err != nil {
		return nil, err
	}
	return pagemanager.NewBufferPool(disk, cacheSize), nil
}

func newTempDB(backendType string) (pagemanager.PageManager, func(), error) {
	dir, err := os.MkdirTemp("", "btree-bench-*")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(dir) }

	path := filepath.Join(dir, "bench.db")
	var pm pagemanager.PageManager
	if backendType == "bp" {
		pm, err = setupBufferPool(path, *cacheSizeFlag)
	} else {
		pm, err = setupPageManager(path)
	}
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("create db: %w", err)
	}
	return pm, cleanup, nil
}

func sampleField() []btree.Field {
	return []btree.Field{{Tag: 1, Value: btree.IntValue{V: 42}}}
}

func collectStats(latencies []time.Duration) (total, p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, d := range sorted {
		total += d
	}
	n := len(sorted)
	p50 = sorted[n/2]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	return
}

func benchWrite(backendType string, ops int, rng *rand.Rand) (result, error) {
	pm, cleanup, err := newTempDB(backendType)
	if err != nil {
		return result{}, err
	}
	defer cleanup()
	defer pm.Close()

	tree := btree.NewBTree(pm)
	field := sampleField()
	latencies := make([]time.Duration, 0, ops)

	for i := 0; i < ops; i++ {
		key := rng.Uint64()
		start := time.Now()
		if err := tree.Insert(key, field); err != nil {
			return result{}, fmt.Errorf("insert: %w", err)
		}
		latencies = append(latencies, time.Since(start))
	}

	total, p50, p95, p99 := collectStats(latencies)
	return result{backend: backendType, mode: "write", ops: ops, total: total, p50: p50, p95: p95, p99: p99}, nil
}

func benchRead(backendType string, ops int, rng *rand.Rand) (result, error) {
	pm, cleanup, err := newTempDB(backendType)
	if err != nil {
		return result{}, err
	}
	defer cleanup()
	defer pm.Close()

	tree := btree.NewBTree(pm)
	field := sampleField()

	// Pre-populate outside the measurement window.
	keys := make([]uint64, ops)
	for i := range keys {
		keys[i] = rng.Uint64()
		if err := tree.Insert(keys[i], field); err != nil {
			return result{}, fmt.Errorf("pre-populate insert: %w", err)
		}
	}

	// Shuffle so access order differs from insert order.
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	latencies := make([]time.Duration, 0, ops)
	for _, key := range keys {
		start := time.Now()
		if _, _, err := tree.Search(key); err != nil {
			return result{}, fmt.Errorf("search: %w", err)
		}
		latencies = append(latencies, time.Since(start))
	}

	total, p50, p95, p99 := collectStats(latencies)
	return result{backend: backendType, mode: "read", ops: ops, total: total, p50: p50, p95: p95, p99: p99}, nil
}

func benchRangeScan(backendType string, ops int, rng *rand.Rand) (result, error) {
	pm, cleanup, err := newTempDB(backendType)
	if err != nil {
		return result{}, err
	}
	defer cleanup()
	defer pm.Close()

	tree := btree.NewBTree(pm)
	field := sampleField()

	// Insert sequential keys spaced by 10 so range queries reliably hit records.
	for i := uint64(0); i < uint64(ops); i++ {
		if err := tree.Insert(i*10, field); err != nil {
			return result{}, fmt.Errorf("pre-populate insert: %w", err)
		}
	}

	keySpace := uint64(ops) * 10
	latencies := make([]time.Duration, 0, ops)
	for i := 0; i < ops; i++ {
		start := rng.Uint64() % keySpace
		end := start + 100
		t := time.Now()
		if _, err := tree.RangeScan(start, end); err != nil {
			return result{}, fmt.Errorf("rangescan: %w", err)
		}
		latencies = append(latencies, time.Since(t))
	}

	total, p50, p95, p99 := collectStats(latencies)
	return result{backend: backendType, mode: "rangescan", ops: ops, total: total, p50: p50, p95: p95, p99: p99}, nil
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	default:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
	}
}

// commaFormat adds thousands separators to a float formatted with no decimals.
func commaFormat(f float64) string {
	s := fmt.Sprintf("%.0f", f)
	n := len(s)
	if n <= 3 {
		return s
	}
	var b strings.Builder
	rem := n % 3
	if rem > 0 {
		b.WriteString(s[:rem])
	}
	for i := rem; i < n; i += 3 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func printTable(results []result) {
	fmt.Printf("\n%-13s %-10s %12s %9s %9s %9s\n",
		"Backend", "Mode", "Ops/sec", "P50", "P95", "P99")
	fmt.Println(strings.Repeat("-", 60))
	for _, r := range results {
		backend := "PageManager"
		if r.backend == "bp" {
			backend = "BufferPool"
		}
		fmt.Printf("%-13s %-10s %12s %9s %9s %9s\n",
			backend, r.mode,
			commaFormat(r.opsPerSec()),
			fmtDur(r.p50), fmtDur(r.p95), fmtDur(r.p99),
		)
	}
}

func printSpeedup(results []result) {
	byMode := make(map[string]map[string]float64)
	for _, r := range results {
		if byMode[r.mode] == nil {
			byMode[r.mode] = make(map[string]float64)
		}
		byMode[r.mode][r.backend] = r.opsPerSec()
	}

	hasBoth := false
	for _, m := range byMode {
		if m["pm"] > 0 && m["bp"] > 0 {
			hasBoth = true
			break
		}
	}
	if !hasBoth {
		return
	}

	fmt.Printf("\nSpeedup (BufferPool vs PageManager):\n")
	for _, mode := range []string{"write", "read", "rangescan"} {
		m := byMode[mode]
		if m["pm"] > 0 && m["bp"] > 0 {
			fmt.Printf("  %-10s %.2fx\n", mode+":", m["bp"]/m["pm"])
		}
	}
}

func run(backendTypes, modes []string, ops int, rng *rand.Rand) []result {
	var results []result
	for _, b := range backendTypes {
		for _, m := range modes {
			label := "PageManager"
			if b == "bp" {
				label = "BufferPool"
			}
			fmt.Printf("  running %-13s %s...\n", label, m)

			var (
				r   result
				err error
			)
			switch m {
			case "write":
				r, err = benchWrite(b, ops, rng)
			case "read":
				r, err = benchRead(b, ops, rng)
			case "rangescan":
				r, err = benchRangeScan(b, ops, rng)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR (%s/%s): %v\n", b, m, err)
				continue
			}
			results = append(results, r)
		}
	}
	return results
}

func main() {
	flag.Parse()

	backends := []string{"pm", "bp"}
	if *backendFlag != "all" {
		backends = []string{*backendFlag}
	}

	modes := []string{"write", "read", "rangescan"}
	if *modeFlag != "all" {
		modes = []string{*modeFlag}
	}

	fmt.Printf("=== BTree Benchmark ===\n")
	fmt.Printf("Ops: %d  Seed: %d  CacheSize: %d\n\n", *opsFlag, *seedFlag, *cacheSizeFlag)

	rng := rand.New(rand.NewSource(*seedFlag))
	results := run(backends, modes, *opsFlag, rng)

	printTable(results)
	printSpeedup(results)
	fmt.Println()
}
