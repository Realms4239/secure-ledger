// Command seed generates a deterministic demo dataset: transfers for the
// loader, a settlement CSV for the engine matcher, and the expected report
// the showcase gate compares against.
//
// Fixed anomaly counts for -n unique transfers (default 10000):
//
//	exact     = n - 195  (clean rows, fee 0 → settled)
//	tolerated = 150      (row amount+fee == saga amount)
//	mismatch  = 25       (row amount beyond tolerance)
//	missing   = 20       (posted, never appears in the CSV)
//	orphans   = 15       (CSV rows for txids never posted, -orphans flag)
//	corrupt   = 10       (malformed CSV lines, -corrupt flag)
//	dups      = 100      (same-key loader retries → gateway 409, -dups flag)
//
// Row timestamps anchor at generation time: the gate regenerates per run so
// engine consume timestamps always fall inside the 5-minute skew window.
// Amounts/order/ids are fully determined by -seed; timestamps are not.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var operators = []string{"mvola", "orange", "airtel"}

type transfer struct {
	From     string  `json:"from"`
	To       string  `json:"to"`
	Amount   float64 `json:"amount"`
	Operator string  `json:"operator"`
	Txid     string  `json:"txid"`
	Key      string  `json:"key"`
}

func main() {
	n := flag.Int("n", 10000, "unique transfers to generate")
	dups := flag.Int("dups", 100, "duplicate-key retries to append")
	orphans := flag.Int("orphans", 15, "orphan CSV rows")
	corrupt := flag.Int("corrupt", 10, "corrupt CSV lines")
	seed := flag.Int64("seed", 42, "rng seed")
	dir := flag.String("dir", "sample-data", "output directory")
	flag.Parse()

	const toleratedN, mismatchN, missingN = 150, 25, 20
	// Sender pool sized so bulk traffic stays under the velocity cap
	// (5/min/sender): n=10000 over 2000 wallets ≈ 5 each. Operator is
	// stable per sender so the geo rule (humans, not loads) never trips.
	const senderPool = 2000
	// 8ms apart: the whole stream fits inside the 5-minute match skew.
	const tsSpacingNs = 8_000_000
	exactN := *n - toleratedN - mismatchN - missingN
	if exactN < 0 {
		fmt.Fprintln(os.Stderr, "seed: -n too small for fixed anomaly counts")
		os.Exit(1)
	}
	rng := rand.New(rand.NewSource(*seed))
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
	nowGen := time.Now().UnixNano()

	var transfers []transfer
	var csvRows []string
	var mismatchTx []string

	emit := func(i int, kind string, amount, fee int, withRow bool) {
		// Sender rotation with a stable operator per sender: bulk seed
		// traffic stays under the velocity cap and never trips the geo
		// rule, which targets humans, not loads. Duplicate-key retries are
		// unaffected (gateway answers 409 before fraud scoring).
		senderIdx := i % senderPool
		from := fmt.Sprintf("user%04d", senderIdx)
		to := fmt.Sprintf("user%04d", rng.Intn(senderPool))
		for to == from {
			to = fmt.Sprintf("user%04d", rng.Intn(senderPool))
		}
		op := operators[senderIdx%len(operators)]
		txid := fmt.Sprintf("tx-%06d", i)
		transfers = append(transfers, transfer{
			From: from, To: to, Amount: float64(amount),
			// Deterministic v4-shaped UUIDs: the gateway requires
			// Idempotency-Key to parse as UUID, and golden files must
			// regenerate identically for the same seed.
			Operator: op, Txid: txid, Key: fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
		})
		if !withRow {
			return
		}
		rowAmt := amount
		switch kind {
		case "tolerated":
			rowAmt = amount - fee
		case "mismatch":
			rowAmt = amount + 5000
			mismatchTx = append(mismatchTx, txid)
		}
		ts := nowGen + int64(i)*tsSpacingNs
		csvRows = append(csvRows, fmt.Sprintf("%s,%s,%s,%s,%d,%d,%d",
			op, txid, from, to, rowAmt, fee, ts))
	}

	idx := 0
	for k := 0; k < exactN; k++ {
		emit(idx, "exact", 100+rng.Intn(49900), 0, true)
		idx++
	}
	for k := 0; k < toleratedN; k++ {
		emit(idx, "tolerated", 1000+rng.Intn(49000), 1+rng.Intn(50), true)
		idx++
	}
	for k := 0; k < mismatchN; k++ {
		emit(idx, "mismatch", 1000+rng.Intn(49000), 0, true)
		idx++
	}
	for k := 0; k < missingN; k++ {
		emit(idx, "missing", 100+rng.Intn(49900), 0, false)
		idx++
	}
	for k := 0; k < *dups; k++ {
		src := transfers[rng.Intn(len(transfers))]
		transfers = append(transfers, src) // same key: loader retry, gateway 409
	}
	var orphanTx []string
	for k := 0; k < *orphans; k++ {
		txid := fmt.Sprintf("orphan-%03d", k)
		orphanTx = append(orphanTx, txid)
		csvRows = append(csvRows, fmt.Sprintf("%s,%s,ghost%d,user%03d,%d,0,%d",
			operators[rng.Intn(len(operators))], txid, k, rng.Intn(200),
			100+rng.Intn(49900), nowGen+int64(idx+k)*tsSpacingNs))
	}

	writeLines := func(name string, lines []string) {
		f, err := os.Create(filepath.Join(*dir, name))
		if err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
		defer f.Close()
		for _, l := range lines {
			fmt.Fprintln(f, l)
		}
	}

	var tl []string
	for _, tr := range transfers {
		raw, _ := json.Marshal(tr)
		tl = append(tl, string(raw))
	}
	writeLines("transfers.jsonl", tl)

	csv := append([]string{"operator,txid,from,to,amount,fee,ts"}, csvRows...)
	for k := 0; k < *corrupt; k++ {
		csv = append(csv, fmt.Sprintf("broken,line,%d", k))
	}
	writeLines("settlement-10k.csv", csv)

	expected := map[string]any{
		"settled":     exactN,
		"tolerated":   toleratedN,
		"missing":     missingN, // count: engine lists saga uuids we can't predict
		"orphan":      orphanTx,
		"mismatch":    mismatchTx,
		"csv_corrupt": *corrupt,
	}
	raw, _ := json.MarshalIndent(expected, "", "  ")
	if err := os.WriteFile(filepath.Join(*dir, "expected-report.json"), append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
	fmt.Printf("seed: %d transfers (%d unique), %d csv rows, settled=%d tolerated=%d\n",
		len(transfers), idx, len(csv)-1, exactN, toleratedN)
}
