package settle

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Coinbase Developer Platform (CDP) API authentication: every request
// carries a short-lived JWT signed with the CDP Secret API key and bound to
// that one method, host and path. Built after GenerateJWT in
// github.com/coinbase/cdp-sdk/go/auth/jwt.go (checked 2026-11-18):
//
//	header: alg EdDSA or ES256, kid = key ID, nonce = 16 random bytes hex, typ JWT
//	claims: sub = key ID, iss = "cdp", nbf = iat = now, exp = now + 120 s,
//	        uris = ["<METHOD> <host><path>"]
//
// The SDK sets no audience, and neither do we.

// cdpJWTLifetime is the SDK's default token lifetime.
const cdpJWTLifetime = 120 * time.Second

// cdpKey is a CDP Secret API key.
type cdpKey struct {
	id     string
	signer crypto.Signer
	alg    string // "EdDSA" or "ES256"
}

// parseCDPKey reads the JSON file CDP hands out for a Secret API key:
// {"id": "<uuid>", "privateKey": "<secret>"} (older keys name the ID
// "name"). The secret is base64 of a 64-byte Ed25519 key (seed and public
// key, CDP's recommended type) or a PEM EC P-256 key.
func parseCDPKey(raw string) (*cdpKey, error) {
	var f struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		PrivateKey string `json:"privateKey"`
	}
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		return nil, errors.New(`cdp key: want the key file's JSON, {"id": ..., "privateKey": ...}`)
	}
	id := f.ID
	if id == "" {
		id = f.Name
	}
	if id == "" || f.PrivateKey == "" {
		return nil, errors.New("cdp key: id and privateKey are required")
	}
	secret := strings.TrimSpace(f.PrivateKey)
	if block, _ := pem.Decode([]byte(secret)); block != nil {
		k, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("cdp key: EC private key: %w", err)
		}
		if k.Curve.Params().Name != "P-256" {
			return nil, errors.New("cdp key: EC key must be P-256 (ES256)")
		}
		return &cdpKey{id: id, signer: k, alg: "ES256"}, nil
	}
	b, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, errors.New("cdp key: privateKey is neither PEM nor base64")
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("cdp key: Ed25519 secret is %d bytes, want %d", len(b), ed25519.PrivateKeySize)
	}
	k := ed25519.PrivateKey(b)
	// The last 32 bytes must be the public key of the first 32: catches a
	// truncated or mangled secret before the first settlement does.
	if !ed25519.NewKeyFromSeed(k.Seed()).Equal(k) {
		return nil, errors.New("cdp key: Ed25519 secret is inconsistent (public half does not match)")
	}
	return &cdpKey{id: id, signer: k, alg: "EdDSA"}, nil
}

// jwt returns a token for one request: method, host (no scheme) and path.
func (k *cdpKey) jwt(method, host, path string, now time.Time) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	header := map[string]any{"alg": k.alg, "kid": k.id, "nonce": hex.EncodeToString(nonce), "typ": "JWT"}
	claims := map[string]any{
		"sub":  k.id,
		"iss":  "cdp",
		"nbf":  now.Unix(),
		"iat":  now.Unix(),
		"exp":  now.Add(cdpJWTLifetime).Unix(),
		"uris": []string{method + " " + host + path},
	}
	h, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	c, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	signing := enc.EncodeToString(h) + "." + enc.EncodeToString(c)

	var sig []byte
	switch key := k.signer.(type) {
	case ed25519.PrivateKey:
		sig = ed25519.Sign(key, []byte(signing))
	case *ecdsa.PrivateKey:
		// JWS ES256 is the raw 32-byte r and s, not ASN.1 DER.
		digest := sha256.Sum256([]byte(signing))
		r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
		if err != nil {
			return "", err
		}
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:])
	default:
		return "", errors.New("cdp key: unsupported key type")
	}
	return signing + "." + enc.EncodeToString(sig), nil
}

// CDPBearer returns an Authorization header value for one request to a CDP
// API: keyFile is the key's JSON, method and rawURL the request's. Used by
// hack/cdpcheck to test credentials without settling anything.
func CDPBearer(keyFile, method, rawURL string, now time.Time) (string, error) {
	k, err := parseCDPKey(keyFile)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "", errors.New("cdp: bad url")
	}
	tok, err := k.jwt(method, u.Host, u.EscapedPath(), now)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok, nil
}
