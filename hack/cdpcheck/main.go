// cdpcheck tests a Coinbase Developer Platform API key against the CDP x402
// facilitator without settling anything: it signs a JWT exactly as the
// settler does and calls GET /supported (free). Prints the HTTP status and
// the networks the facilitator supports; never prints the key.
//
//	go run ./hack/cdpcheck -key cdp-api-key.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"apivend.io/apivend/internal/settle"
)

func main() {
	keyPath := flag.String("key", "", "CDP API key file ({\"id\": ..., \"privateKey\": ...})")
	base := flag.String("url", "https://api.cdp.coinbase.com/platform/v2/x402", "facilitator base URL")
	flag.Parse()
	raw, err := os.ReadFile(*keyPath)
	if err != nil {
		fail("read key: %v", err)
	}
	url := strings.TrimRight(*base, "/") + "/supported"
	auth, err := settle.CDPBearer(string(raw), http.MethodGet, url, time.Now())
	if err != nil {
		fail("%v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", auth)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		fail("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	fmt.Println("HTTP", resp.StatusCode)
	var sup struct {
		Kinds []struct {
			X402Version int             `json:"x402Version"`
			Scheme      string          `json:"scheme"`
			Network     string          `json:"network"`
			Extra       json.RawMessage `json:"extra"`
		} `json:"kinds"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &sup) != nil {
		// An error body from CDP (errorType, errorMessage, correlationId).
		fmt.Println(strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	var kinds []string
	for _, k := range sup.Kinds {
		line := fmt.Sprintf("v%d %s %s", k.X402Version, k.Scheme, k.Network)
		if len(k.Extra) > 0 && string(k.Extra) != "null" {
			line += " " + string(k.Extra) // public terms (addresses, delays), never the key
		}
		kinds = append(kinds, line)
	}
	sort.Strings(kinds)
	fmt.Println(strings.Join(kinds, "\n"))
}

func fail(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "cdpcheck: "+f+"\n", a...)
	os.Exit(1)
}
