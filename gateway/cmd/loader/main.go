// Command loader fires seeded transfers at the gateway and reports edge
// latency. Each line of -transfers is
// {from,to,amount,operator,txid,key}; the JWT sub equals the sender so the
// gateway policy passes. Repeated keys exercise the 409 dedup path.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type transfer struct {
	From     string  `json:"from"`
	To       string  `json:"to"`
	Amount   float64 `json:"amount"`
	Operator string  `json:"operator"`
	Txid     string  `json:"txid"`
	Key      string  `json:"key"`
}

type summary struct {
	N        int     `json:"n"`
	Accepted int     `json:"accepted"`
	Held     int     `json:"held_for_review"`
	Dup      int     `json:"duplicate"`
	Other    int     `json:"other"`
	Seconds  float64 `json:"seconds"`
	RPS      float64 `json:"rps"`
	P50ms    float64 `json:"p50_ms"`
	P95ms    float64 `json:"p95_ms"`
	P99ms    float64 `json:"p99_ms"`
}

func main() {
	url := flag.String("url", "http://localhost:8080", "gateway base URL")
	secret := flag.String("jwt-secret", "", "HS256 secret (required)")
	path := flag.String("transfers", "sample-data/transfers.jsonl", "seed transfers file")
	conc := flag.Int("c", 50, "concurrency")
	out := flag.String("out", "", "write summary JSON here (default stdout only)")
	tokenSub := flag.String("token", "", "print one HS256 JWT for sub and exit (for scripts)")
	flag.Parse()
	if *tokenSub != "" {
		if *secret == "" {
			fmt.Fprintln(os.Stderr, "loader: -jwt-secret required")
			os.Exit(1)
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": *tokenSub, "exp": time.Now().Add(time.Hour).Unix(),
		})
		s, err := tok.SignedString([]byte(*secret))
		if err != nil {
			fmt.Fprintln(os.Stderr, "loader: sign:", err)
			os.Exit(1)
		}
		fmt.Println(s)
		return
	}
	if *secret == "" {
		fmt.Fprintln(os.Stderr, "loader: -jwt-secret required")
		os.Exit(1)
	}

	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loader:", err)
		os.Exit(1)
	}
	defer f.Close()
	var all []transfer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var tr transfer
		if err := json.Unmarshal(sc.Bytes(), &tr); err != nil {
			fmt.Fprintln(os.Stderr, "loader: bad transfers line:", err)
			os.Exit(1)
		}
		all = append(all, tr)
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "loader:", err)
		os.Exit(1)
	}

	tokens := sync.Map{} // sender -> bearer (mint once per sender)
	bearer := func(sender string) string {
		if v, ok := tokens.Load(sender); ok {
			return v.(string)
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": sender, "exp": time.Now().Add(time.Hour).Unix(),
		})
		s, err := tok.SignedString([]byte(*secret))
		if err != nil {
			fmt.Fprintln(os.Stderr, "loader: sign:", err)
			os.Exit(1)
		}
		tokens.Store(sender, s)
		return s
	}

	jobs := make(chan transfer)
	var mu sync.Mutex
	var lat []float64
	sum := summary{N: len(all)}
	client := &http.Client{Timeout: 30 * time.Second}
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tr := range jobs {
				body, _ := json.Marshal(map[string]any{
					"from_account": tr.From, "to_account": tr.To,
					"amount": tr.Amount, "operator": tr.Operator, "txid": tr.Txid,
				})
				start := time.Now()
				req, _ := http.NewRequest("POST", *url+"/transfers", bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+bearer(tr.From))
				req.Header.Set("Idempotency-Key", tr.Key)
				resp, err := client.Do(req)
				el := float64(time.Since(start).Microseconds()) / 1000.0
				mu.Lock()
				lat = append(lat, el)
				if err != nil {
					sum.Other++
					mu.Unlock()
					continue
				}
				var v map[string]string
				_ = json.NewDecoder(resp.Body).Decode(&v)
				resp.Body.Close()
				switch {
				case resp.StatusCode == 202 && v["status"] == "held_for_review":
					sum.Held++
				case resp.StatusCode == 202:
					sum.Accepted++
				case resp.StatusCode == 409:
					sum.Dup++
				default:
					sum.Other++
				}
				mu.Unlock()
			}
		}()
	}
	start := time.Now()
	for _, tr := range all {
		jobs <- tr
	}
	close(jobs)
	wg.Wait()
	sum.Seconds = time.Since(start).Seconds()
	sum.RPS = float64(sum.N) / sum.Seconds
	sort.Float64s(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		i := int(p * float64(len(lat)-1))
		return lat[i]
	}
	sum.P50ms, sum.P95ms, sum.P99ms = pct(0.5), pct(0.95), pct(0.99)
	raw, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(raw))
	if *out != "" {
		if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "loader:", err)
			os.Exit(1)
		}
	}
}
