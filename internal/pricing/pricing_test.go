package pricing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func route(name, prefix string, hosts ...string) RouteConfig {
	return RouteConfig{
		Name:    name,
		Match:   MatchConfig{PathPrefix: prefix, Hosts: hosts},
		Price:   PriceConfig{Amount: "0.01", Currency: "USD"},
		Accepts: []AcceptConfig{{Network: "base", Asset: "USDC", PayTo: "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"}},
	}
}

func TestMatch(t *testing.T) {
	table, err := Build(Config{Routes: []RouteConfig{
		route("search", "/search"),
		route("deep", "/search/deep"),
		route("hosted", "/v1", "api.example.com"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ host, path, want string }{
		{"any", "/search", "search"},
		{"any", "/search/", "search"},
		{"any", "/search/x?q=1", "search"},
		{"any", "/search?q=/search/deep", "search"},
		{"any", "/search/deep/er", "deep"},
		{"any", "/searching", ""}, // segment boundary, as Gateway API PathPrefix
		{"any", "/", ""},
		{"api.example.com", "/v1/items", "hosted"},
		{"API.example.com:8443", "/v1/items", "hosted"},
		{"other.example.com", "/v1/items", ""},
	}
	for _, tc := range cases {
		got := ""
		if r := table.Match(tc.host, tc.path, ""); r != nil {
			got = r.Name
		}
		if got != tc.want {
			t.Errorf("Match(%q, %q) = %q, want %q", tc.host, tc.path, got, tc.want)
		}
	}
}

func TestOfferResolution(t *testing.T) {
	table, err := Build(Config{Routes: []RouteConfig{route("search", "/search")}})
	if err != nil {
		t.Fatal(err)
	}
	o := table.Match("", "/search", "").Offers[0]
	if o.Wire.Amount != "10000" || o.Wire.Network != "eip155:8453" || o.Wire.Scheme != "exact" {
		t.Fatalf("wire = %+v", o.Wire)
	}
	if o.Wire.Asset != "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913" || o.Wire.MaxTimeoutSeconds != 60 {
		t.Fatalf("wire = %+v", o.Wire)
	}
	if o.Domain.ChainID.Int64() != 8453 || o.Domain.Name != "USD Coin" {
		t.Fatalf("domain = %+v", o.Domain)
	}
}

func TestToAtomic(t *testing.T) {
	ok := map[string]string{"0.01": "10000", "1": "1000000", "1.5": "1500000", ".5": "500000", "0.000001": "1", "12.345678": "12345678"}
	for in, want := range ok {
		got, err := toAtomic(in, 6)
		if err != nil || got.String() != want {
			t.Errorf("toAtomic(%q) = %v, %v; want %s", in, got, err, want)
		}
	}
	for _, in := range []string{"", ".", "0", "0.0", "-1", "1e3", "0.0000001", "1.2.3", "abc", " 1"} {
		if got, err := toAtomic(in, 6); err == nil {
			t.Errorf("toAtomic(%q) = %v, want error", in, got)
		}
	}
}

func TestBuildRejects(t *testing.T) {
	mut := func(f func(*RouteConfig)) Config {
		r := route("r", "/r")
		f(&r)
		return Config{Routes: []RouteConfig{r}}
	}
	cases := map[string]Config{
		"no name":         mut(func(r *RouteConfig) { r.Name = "" }),
		"relative prefix": mut(func(r *RouteConfig) { r.Match.PathPrefix = "r" }),
		"currency":        mut(func(r *RouteConfig) { r.Price.Currency = "EUR" }),
		"zero price":      mut(func(r *RouteConfig) { r.Price.Amount = "0" }),
		"no accepts":      mut(func(r *RouteConfig) { r.Accepts = nil }),
		"unknown network": mut(func(r *RouteConfig) { r.Accepts[0].Network = "radius" }),
		"unknown asset":   mut(func(r *RouteConfig) { r.Accepts[0].Asset = "DAI" }),
		"placeholder pay": mut(func(r *RouteConfig) { r.Accepts[0].PayTo = "0xYourAddress" }),
		"zero payTo":      mut(func(r *RouteConfig) { r.Accepts[0].PayTo = "0x0000000000000000000000000000000000000000" }),
		"duplicate name":  {Routes: []RouteConfig{route("r", "/a"), route("r", "/b")}},
	}
	for name, cfg := range cases {
		if _, err := Build(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadStrict(t *testing.T) {
	dir := t.TempDir()
	good := `
routes:
  - name: search
    match: {pathPrefix: /search}
    price: {amount: "0.01", currency: USD}
    accepts:
      - {network: base-sepolia, asset: USDC, payTo: "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"}
`
	p := filepath.Join(dir, "routes.yaml")
	os.WriteFile(p, []byte(good), 0o600)
	if table, err := Load(p); err != nil || table.Len() != 1 {
		t.Fatalf("Load: %v", err)
	}
	// A misspelt field must not silently make a route free or mispriced.
	os.WriteFile(p, []byte(strings.Replace(good, "price:", "prize:", 1)), 0o600)
	if _, err := Load(p); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestMatchByEnvoyRoute(t *testing.T) {
	byRoute := route("search", "")
	byRoute.Match = MatchConfig{EnvoyRoutePrefix: "httproute/prod/search/"}
	both := route("admin", "/admin")
	both.Match.EnvoyRoutePrefix = "httproute/prod/search/"
	table, err := Build(Config{Routes: []RouteConfig{byRoute, both}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ path, envoyRoute, want string }{
		{"/anything", "httproute/prod/search/rule/0/match/0/api_example_com", "search"},
		{"/admin/x", "httproute/prod/search/rule/1/match/0", "admin"}, // path narrows further
		{"/anything", "httproute/prod/search-v2/rule/0/match/0", ""},  // trailing slash guards the name
		{"/anything", "httproute/dev/search/rule/0/match/0", ""},
		{"/anything", "", ""}, // attribute not sent: never priced by accident, never free by accident either
	}
	for _, tc := range cases {
		got := ""
		if r := table.Match("h", tc.path, tc.envoyRoute); r != nil {
			got = r.Name
		}
		if got != tc.want {
			t.Errorf("Match(%q, %q) = %q, want %q", tc.path, tc.envoyRoute, got, tc.want)
		}
	}
	if _, err := Build(Config{Routes: []RouteConfig{func() RouteConfig { r := route("x", ""); return r }()}}); err == nil {
		t.Error("route without any match accepted")
	}
}

func TestDiscovery(t *testing.T) {
	const bazaar = `{ "info": {"input": {"type": "http", "method": "GET"}}, "schema": {"type": "object"} }`
	described := func(f func(*RouteConfig)) Config {
		r := route("r", "/r")
		r.Description = "Search the web"
		r.Discovery = &DiscoveryConfig{Publish: true, ServiceName: "Search", Tags: []string{"search"}, IconURL: "https://example.com/i.png", Bazaar: json.RawMessage(bazaar)}
		f(&r)
		return Config{Routes: []RouteConfig{r}}
	}
	table, err := Build(described(func(*RouteConfig) {}))
	if err != nil {
		t.Fatal(err)
	}
	d := table.Match("", "/r", "").Discovery
	if d == nil || !d.Publish || d.ServiceName != "Search" || string(d.Bazaar) != `{"info":{"input":{"type":"http","method":"GET"}},"schema":{"type":"object"}}` {
		t.Fatalf("discovery = %+v", d)
	}
	for name, cfg := range map[string]Config{
		"no description":   described(func(r *RouteConfig) { r.Description = "" }),
		"long description": described(func(r *RouteConfig) { r.Description = strings.Repeat("x", 501) }),
		"long name":        described(func(r *RouteConfig) { r.Discovery.ServiceName = strings.Repeat("x", 33) }),
		"non-ascii name":   described(func(r *RouteConfig) { r.Discovery.ServiceName = "Süche" }),
		"six tags":         described(func(r *RouteConfig) { r.Discovery.Tags = []string{"a", "b", "c", "d", "e", "f"} }),
		"http icon":        described(func(r *RouteConfig) { r.Discovery.IconURL = "http://example.com/i.png" }),
		"no bazaar":        described(func(r *RouteConfig) { r.Discovery.Bazaar = nil }),
		"no input":         described(func(r *RouteConfig) { r.Discovery.Bazaar = json.RawMessage(`{"info":{},"schema":{}}`) }),
		"no schema":        described(func(r *RouteConfig) { r.Discovery.Bazaar = json.RawMessage(`{"info":{"input":{}}}`) }),
		"not json":         described(func(r *RouteConfig) { r.Discovery.Bazaar = json.RawMessage(`{"info":`) }),
	} {
		if _, err := Build(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
