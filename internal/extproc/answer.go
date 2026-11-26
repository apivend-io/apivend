package extproc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"mime"
	"strings"
	"sync"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"

	"apivend.io/apivend/internal/pricing"
)

// AnswerCheck is what the filter does with a paid 2xx answer before it
// charges for it.
type AnswerCheck string

const (
	// AnswerCheckOff charges on the response headers, unchecked.
	AnswerCheckOff AnswerCheck = "off"
	// AnswerCheckWatch checks the answer and counts the verdict, but
	// charges as usual.
	AnswerCheckWatch AnswerCheck = "watch"
	// AnswerCheckEnforce does not charge for an unusable answer: the
	// reservation is released and the buyer gets an error, not the answer.
	AnswerCheckEnforce AnswerCheck = "enforce"
)

// ParseAnswerCheck accepts off, watch and enforce.
func ParseAnswerCheck(s string) (AnswerCheck, bool) {
	switch c := AnswerCheck(s); c {
	case AnswerCheckOff, AnswerCheckWatch, AnswerCheckEnforce:
		return c, true
	}
	return "", false
}

// DefaultAnswerMaxBytes is the largest answer that is checked; a larger one
// is charged unchecked.
const DefaultAnswerMaxBytes = 256 << 10

// Verdicts. The check looks at the form of an answer, never at its content:
// an empty result in the normal structure is an answer and is charged.
const (
	VerdictOK            = "ok"
	VerdictEmpty         = "empty"          // no body on a 2xx (not 204)
	VerdictNotJSON       = "not_json"       // JSON expected, does not parse
	VerdictHTML          = "html"           // JSON expected, an HTML page came
	VerdictErrorOnly     = "error_only"     // only error keys, with an error signal
	VerdictMissingFields = "missing_fields" // lacks half of the fields the route reliably returns
	VerdictUncheckedSize = "unchecked_size" // over the cap
	VerdictUncheckedEnc  = "unchecked_encoding"
	VerdictUncheckedType = "unchecked_type" // not JSON, not declared JSON
	VerdictNoBody        = "no_body"        // the body was asked for but never came
)

// refusing reports whether a verdict means the answer is unusable.
func refusing(v string) bool {
	switch v {
	case VerdictEmpty, VerdictNotJSON, VerdictHTML, VerdictErrorOnly, VerdictMissingFields:
		return true
	}
	return false
}

// answerPlan is what the response headers say about the answer to come.
type answerPlan struct {
	check   bool   // ask Envoy for the body
	verdict string // when !check: why not
	gzip    bool
}

// planAnswer decides, from the response headers, whether the answer is to
// be checked.
func planAnswer(route *pricing.Route, code int, head bool, hdr map[string]string) answerPlan {
	if code == 204 || code == 205 || head {
		return answerPlan{verdict: VerdictUncheckedType}
	}
	ct, _, _ := mime.ParseMediaType(hdr["content-type"])
	ct = strings.ToLower(ct)
	isJSON := ct == "application/json" || strings.HasSuffix(ct, "+json")
	declared := declaresJSON(route)
	switch {
	case ct == "text/event-stream":
		return answerPlan{verdict: VerdictUncheckedType}
	case !isJSON && !declared:
		return answerPlan{verdict: VerdictUncheckedType}
	}
	p := answerPlan{check: true}
	switch strings.ToLower(strings.TrimSpace(hdr["content-encoding"])) {
	case "", "identity":
	case "gzip", "x-gzip":
		p.gzip = true
	default:
		return answerPlan{verdict: VerdictUncheckedEnc}
	}
	return p
}

// answerEncoding adds, with the check on, an Accept-Encoding the check can
// read to a paid request's headers: gzip when the buyer takes gzip, else
// identity. Proxies in front of the gateway (Cloudflare among them) offer
// the upstream br and zstd on the buyer's behalf, which would leave most
// answers unchecked; they compress again on the way out. A buyer who
// refuses both gzip and identity is left as they are.
func (s *Server) answerEncoding(hdr map[string]string, set ...*corev3.HeaderValueOption) []*corev3.HeaderValueOption {
	if s.answer == AnswerCheckOff {
		return set
	}
	if enc := readableEncoding(hdr["accept-encoding"]); enc != "" {
		set = append(set, setHeader("accept-encoding", enc))
	}
	return set
}

// readableEncoding: "gzip" if an Accept-Encoding value takes gzip, else
// "identity" if it takes that, else "".
func readableEncoding(accept string) string {
	if strings.TrimSpace(accept) == "" {
		// No header: RFC 9110 allows any encoding, but simple clients
		// (curl, urllib) cannot decode one they did not ask for.
		return "identity"
	}
	q := map[string]bool{} // coding -> acceptable, as listed
	for _, part := range strings.Split(accept, ",") {
		coding, params, _ := strings.Cut(part, ";")
		coding = strings.ToLower(strings.TrimSpace(coding))
		ok := true
		for _, p := range strings.Split(params, ";") {
			k, v, _ := strings.Cut(strings.TrimSpace(p), "=")
			if strings.EqualFold(k, "q") {
				v = strings.TrimSpace(v)
				ok = strings.Trim(v, "0.") != ""
			}
		}
		if coding == "x-gzip" {
			coding = "gzip"
		}
		if coding != "" {
			q[coding] = ok
		}
	}
	takes := func(c string) bool {
		if ok, listed := q[c]; listed {
			return ok
		}
		if ok, listed := q["*"]; listed {
			return ok
		}
		return c == "identity" // identity is acceptable unless refused
	}
	switch {
	case takes("gzip"):
		return "gzip"
	case takes("identity"):
		return "identity"
	}
	return ""
}

// declaresJSON: the route's discovery output type is json.
func declaresJSON(route *pricing.Route) bool {
	out := routeOutput(route)
	return out != nil && (out.Type == "" || strings.EqualFold(out.Type, "json"))
}

type bazaarOutput struct {
	Type    string          `json:"type"`
	Example json.RawMessage `json:"example"`
}

func routeOutput(route *pricing.Route) *bazaarOutput {
	if route == nil || route.Discovery == nil || len(route.Discovery.Bazaar) == 0 {
		return nil
	}
	var b struct {
		Info struct {
			Output *bazaarOutput `json:"output"`
		} `json:"info"`
	}
	if json.Unmarshal(route.Discovery.Bazaar, &b) != nil {
		return nil
	}
	return b.Info.Output
}

// errorKeys: an object made only of these, with an error signal, is an
// error dressed as a 200.
var errorKeys = map[string]bool{
	"error": true, "errors": true, "message": true, "msg": true, "detail": true, "details": true,
	"status": true, "statusCode": true, "status_code": true, "code": true, "success": true, "ok": true,
	"reason": true, "title": true, "type": true, "error_code": true, "error_message": true,
	"errorCode": true, "errorMessage": true, "request_id": true, "requestId": true, "timestamp": true,
}

// errorShaped reports whether an object is only error keys and carries an
// error signal.
func errorShaped(obj map[string]json.RawMessage) bool {
	if len(obj) == 0 {
		return false
	}
	for k := range obj {
		if !errorKeys[k] {
			return false
		}
	}
	for _, k := range []string{"error", "errors"} {
		if v, ok := obj[k]; ok && truthy(v) {
			return true
		}
	}
	for _, k := range []string{"success", "ok"} {
		if v, ok := obj[k]; ok && bytes.Equal(bytes.TrimSpace(v), []byte("false")) {
			return true
		}
	}
	if v, ok := obj["status"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			switch strings.ToLower(s) {
			case "error", "fail", "failed", "failure":
				return true
			}
		}
	}
	return false
}

// truthy: not null, false, "", [], {} or 0.
func truthy(v json.RawMessage) bool {
	switch string(bytes.TrimSpace(v)) {
	case "", "null", "false", `""`, "[]", "{}", "0":
		return false
	}
	return true
}

// checkAnswer gives the verdict on a buffered answer. complete is false when
// Envoy sent only part of the body (over its buffer limit).
func (s *Server) checkAnswer(route *pricing.Route, p answerPlan, body []byte, complete bool) string {
	if !complete || len(body) > s.answerMax {
		return VerdictUncheckedSize
	}
	if p.gzip && len(body) > 0 {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return VerdictNotJSON
		}
		// One member: bytes after it (padding) are ignored, as clients do.
		zr.Multistream(false)
		plain, err := io.ReadAll(io.LimitReader(zr, int64(s.answerMax)+1))
		if err != nil {
			return VerdictNotJSON
		}
		if len(plain) > s.answerMax {
			return VerdictUncheckedSize
		}
		body = plain
	}
	body = bytes.TrimSpace(bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))) // a UTF-8 BOM is not a broken answer
	switch {
	case len(body) == 0:
		return VerdictEmpty
	case body[0] == '<':
		// The body decides, not the content type: valid JSON sent as
		// text/html is an answer.
		return VerdictHTML
	case !json.Valid(body):
		return VerdictNotJSON
	}
	var obj map[string]json.RawMessage
	if body[0] != '{' || json.Unmarshal(body, &obj) != nil {
		return VerdictOK // an array or a scalar: the form is all we check
	}
	if errorShaped(obj) && !exampleErrorShaped(route) {
		return VerdictErrorOnly
	}
	return s.shapes.observe(route.Name, obj)
}

// exampleErrorShaped: the seller's own example looks like an error, so
// that is their normal answer.
func exampleErrorShaped(route *pricing.Route) bool {
	out := routeOutput(route)
	if out == nil || len(out.Example) == 0 {
		return false
	}
	var obj map[string]json.RawMessage
	return json.Unmarshal(out.Example, &obj) == nil && errorShaped(obj)
}

// Learning which top-level fields a route really always returns: the last
// shapeWindow object answers, per route and per filter replica, in memory.
// A field is reliable once shapeMinSeen answers were seen and it was in at
// least 99% of the window. Every non-error answer is learned, refused ones
// included, so a seller who changes their answer's shape is refused for a
// handful of answers at most.
const (
	shapeWindow  = 200
	shapeMinSeen = 50
	shapeMaxKeys = 64
	shapeRoutes  = 10000
)

type shapes struct {
	mu     sync.Mutex
	routes map[string]*shape
}

type shape struct {
	keys   map[string]uint // key -> bit
	names  []string        // bit -> key
	ring   [shapeWindow]uint64
	n, at  int
	counts [shapeMaxKeys]int
}

func (sh *shapes) observe(route string, obj map[string]json.RawMessage) string {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.routes == nil {
		sh.routes = map[string]*shape{}
	}
	r := sh.routes[route]
	if r == nil {
		if len(sh.routes) >= shapeRoutes {
			return VerdictOK
		}
		r = &shape{keys: map[string]uint{}}
		sh.routes[route] = r
	}
	verdict := VerdictOK
	if r.n >= shapeMinSeen {
		need := (r.n*99 + 99) / 100 // ceil(99%)
		reliable, missing := 0, 0
		for bit, name := range r.names {
			if r.counts[bit] >= need {
				reliable++
				if _, ok := obj[name]; !ok {
					missing++
				}
			}
		}
		if missing > 0 && missing*2 >= reliable {
			verdict = VerdictMissingFields
		}
	}
	var bits uint64
	for k := range obj {
		b, ok := r.keys[k]
		if !ok {
			if len(r.names) >= shapeMaxKeys {
				continue
			}
			b = uint(len(r.names))
			r.keys[k] = b
			r.names = append(r.names, k)
		}
		bits |= 1 << b
	}
	if r.n == shapeWindow {
		old := r.ring[r.at]
		for b := range r.names {
			if old&(1<<uint(b)) != 0 {
				r.counts[b]--
			}
		}
	} else {
		r.n++
	}
	r.ring[r.at] = bits
	r.at = (r.at + 1) % shapeWindow
	for b := range r.names {
		if bits&(1<<uint(b)) != 0 {
			r.counts[b]++
		}
	}
	return verdict
}
