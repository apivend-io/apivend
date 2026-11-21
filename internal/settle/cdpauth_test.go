package settle

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testKeyID = "3f1c2a9e-0d4b-4c1e-9a7f-1b2c3d4e5f60"

func ed25519KeyFile(t *testing.T) (string, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := json.Marshal(map[string]string{"id": testKeyID, "privateKey": base64.StdEncoding.EncodeToString(priv)})
	return string(f), pub
}

func ecKeyFile(t *testing.T) (string, *ecdsa.PublicKey) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalECPrivateKey(k)
	p := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
	f, _ := json.Marshal(map[string]string{"name": "organizations/o/apiKeys/" + testKeyID, "privateKey": string(p)})
	return string(f), &k.PublicKey
}

// checkJWT verifies a token as CDP would and returns its header and claims.
func checkJWT(t *testing.T, tok string, pub any) (map[string]any, map[string]any) {
	t.Helper()
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWS: %q", tok)
	}
	dec := base64.RawURLEncoding
	var header, claims map[string]any
	hb, _ := dec.DecodeString(parts[0])
	cb, _ := dec.DecodeString(parts[1])
	sig, err := dec.DecodeString(parts[2])
	if err != nil || json.Unmarshal(hb, &header) != nil || json.Unmarshal(cb, &claims) != nil {
		t.Fatalf("undecodable token")
	}
	signing := []byte(parts[0] + "." + parts[1])
	switch k := pub.(type) {
	case ed25519.PublicKey:
		if header["alg"] != "EdDSA" || !ed25519.Verify(k, signing, sig) {
			t.Fatalf("EdDSA signature does not verify (alg %v)", header["alg"])
		}
	case *ecdsa.PublicKey:
		if header["alg"] != "ES256" || len(sig) != 64 {
			t.Fatalf("ES256 wants a 64-byte r||s signature (alg %v, %d bytes)", header["alg"], len(sig))
		}
		d := sha256.Sum256(signing)
		r, s := new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])
		if !ecdsa.Verify(k, d[:], r, s) {
			t.Fatal("ES256 signature does not verify")
		}
	}
	return header, claims
}

func TestCDPJWT(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	edFile, edPub := ed25519KeyFile(t)
	ecFile, ecPub := ecKeyFile(t)
	for name, tc := range map[string]struct {
		file   string
		pub    any
		wantID string
	}{
		"ed25519": {edFile, edPub, testKeyID},
		"es256":   {ecFile, ecPub, "organizations/o/apiKeys/" + testKeyID},
	} {
		t.Run(name, func(t *testing.T) {
			k, err := parseCDPKey(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			tok, err := k.jwt("POST", "api.cdp.coinbase.com", "/platform/v2/x402/settle", now)
			if err != nil {
				t.Fatal(err)
			}
			h, c := checkJWT(t, tok, tc.pub)
			if h["kid"] != tc.wantID || h["typ"] != "JWT" || len(fmt.Sprint(h["nonce"])) != 32 {
				t.Errorf("header %v", h)
			}
			if c["sub"] != tc.wantID || c["iss"] != "cdp" || c["nbf"] != float64(now.Unix()) ||
				c["exp"] != float64(now.Add(120*time.Second).Unix()) {
				t.Errorf("claims %v", c)
			}
			if uris, _ := c["uris"].([]any); len(uris) != 1 || uris[0] != "POST api.cdp.coinbase.com/platform/v2/x402/settle" {
				t.Errorf("uris %v", c["uris"])
			}
			if _, ok := c["aud"]; ok {
				t.Error("the SDK sets no audience")
			}
			tok2, _ := k.jwt("POST", "api.cdp.coinbase.com", "/platform/v2/x402/settle", now)
			if tok2 == tok {
				t.Error("two tokens share a nonce")
			}
		})
	}
}

func TestParseCDPKeyRejects(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	mangled := append([]byte(nil), priv...)
	mangled[40] ^= 1 // public half no longer matches the seed
	k384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	der, _ := x509.MarshalECPrivateKey(k384)
	p384 := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
	file := func(id, key string) string {
		b, _ := json.Marshal(map[string]string{"id": id, "privateKey": key})
		return string(b)
	}
	for name, raw := range map[string]string{
		"not json":   "abc:def",
		"no id":      file("", base64.StdEncoding.EncodeToString(priv)),
		"no key":     file(testKeyID, ""),
		"not base64": file(testKeyID, "!!!"),
		"short":      file(testKeyID, base64.StdEncoding.EncodeToString(priv[:32])),
		"mangled":    file(testKeyID, base64.StdEncoding.EncodeToString(mangled)),
		"p384":       file(testKeyID, p384),
	} {
		if _, err := parseCDPKey(raw); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), base64.StdEncoding.EncodeToString(priv)[:16]) {
			t.Errorf("%s: error message leaks the key", name)
		}
	}
}

// The facilitator backend sends a fresh JWT per request, bound to the
// settle endpoint's host and path, and never the key itself.
func TestFacilitatorSendsCDPJWT(t *testing.T) {
	file, pub := ed25519KeyFile(t)
	var got []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		if r.URL.Path != "/platform/v2/x402/settle" {
			t.Errorf("path %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"success":true,"transaction":"0xabc","network":"eip155:8453"}`)
	}))
	defer srv.Close()

	backends, err := BuildProviders(ProvidersConfig{Providers: []ProviderConfig{{Name: "cdp", Backends: []BackendConfig{
		{Type: "facilitator", URL: srv.URL + "/platform/v2/x402", Token: file, Auth: "cdp"},
	}}}}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f := backends["cdp"][0].(*Facilitator)
	f.client = srv.Client()
	for i := 0; i < 2; i++ {
		if r := f.Settle(t.Context(), &Job{}); r.Outcome != Settled {
			t.Fatalf("settle: %+v", r)
		}
	}
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("want two distinct tokens, got %d", len(got))
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	for _, a := range got {
		if strings.Contains(a, "privateKey") || !strings.HasPrefix(a, "Bearer ") {
			t.Fatalf("authorization header %q", a)
		}
		_, c := checkJWT(t, strings.TrimPrefix(a, "Bearer "), pub)
		if uris, _ := c["uris"].([]any); len(uris) != 1 || uris[0] != "POST "+host+"/platform/v2/x402/settle" {
			t.Fatalf("uris %v", c["uris"])
		}
	}
}

func TestCDPAuthConfigErrors(t *testing.T) {
	file, _ := ed25519KeyFile(t)
	for name, bc := range map[string]BackendConfig{
		"no credentials": {Type: "facilitator", URL: "https://api.cdp.coinbase.com/platform/v2/x402", Auth: "cdp"},
		"plain http":     {Type: "facilitator", URL: "http://api.cdp.coinbase.com/platform/v2/x402", Auth: "cdp", Token: file},
		"unknown auth":   {Type: "facilitator", URL: "https://x402.org/facilitator", Auth: "basic", Token: "t"},
	} {
		if _, err := BuildProviders(ProvidersConfig{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{bc}}}}, time.Second); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// CDP's 402 means our account needs billing: not the payer's fault, retried.
func TestFacilitatorBillingIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		fmt.Fprint(w, `{"errorType":"payment_method_required","errorMessage":"Please add a payment method"}`)
	}))
	defer srv.Close()
	f, err := newFacilitator(BackendConfig{Type: "facilitator", URL: srv.URL}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if r := f.Settle(t.Context(), &Job{}); r.Outcome != Unavailable || r.Reason != "facilitator_billing" {
		t.Fatalf("%+v", r)
	}
}

// A payer refusal can also arrive on 402; it carries an errorReason and must
// stay the payer's refusal (which blocks the payer), not a billing problem.
func TestPayerRefusalOn402IsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		fmt.Fprint(w, `{"success":false,"errorReason":"insufficient_funds"}`)
	}))
	defer srv.Close()
	f, _ := newFacilitator(BackendConfig{Type: "facilitator", URL: srv.URL}, time.Second)
	if r := f.Settle(t.Context(), &Job{}); r.Outcome != Refused || r.Reason != "insufficient_funds" {
		t.Fatalf("%+v", r)
	}
}

// settlement_pending arrives on HTTP 500 from CDP with the transaction set:
// non-terminal, tracked on chain, never a failure.
func TestSettlementPendingOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"success":false,"errorReason":"settlement_pending","transaction":"0xdef","network":"eip155:8453"}`)
	}))
	defer srv.Close()
	f, _ := newFacilitator(BackendConfig{Type: "facilitator", URL: srv.URL}, time.Second)
	if r := f.Settle(t.Context(), &Job{}); r.Outcome != Pending || r.Transaction != "0xdef" {
		t.Fatalf("%+v", r)
	}
}
