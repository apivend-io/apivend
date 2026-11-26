package extproc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"apivend.io/apivend/internal/pricing"
)

func jsonRoute(name, example string) *pricing.Route {
	b := `{"info":{"output":{"type":"json"}}}`
	if example != "" {
		b = `{"info":{"output":{"type":"json","example":` + example + `}}}`
	}
	return &pricing.Route{Name: name, Discovery: &pricing.Discovery{Bazaar: json.RawMessage(b)}}
}

func gz(s string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(s))
	w.Close()
	return buf.Bytes()
}

func TestCheckAnswerVerdicts(t *testing.T) {
	s := &Server{answerMax: 1024}
	plain := answerPlan{check: true}
	for _, tc := range []struct {
		name, body string
		plan       answerPlan
		complete   bool
		want       string
	}{
		{"empty list is an answer", `{"results":[]}`, plain, true, VerdictOK},
		{"not found in the normal structure", `{"taken":false,"name":"x"}`, plain, true, VerdictOK},
		{"nulls are an answer", `{"price":null}`, plain, true, VerdictOK},
		{"status ok", `{"status":"ok"}`, plain, true, VerdictOK},
		{"array", `[]`, plain, true, VerdictOK},
		{"scalar", `0`, plain, true, VerdictOK},
		{"empty body", "", plain, true, VerdictEmpty},
		{"whitespace", " \n", plain, true, VerdictEmpty},
		{"cut off", `{"results":[1,2`, plain, true, VerdictNotJSON},
		{"html page", `<!doctype html><title>Down for maintenance</title>`, plain, true, VerdictHTML},
		{"JSON sent as text/html is an answer", `{"a":1}`, plain, true, VerdictOK},
		{"error dressed as 200", `{"error":"rate limited"}`, plain, true, VerdictErrorOnly},
		{"success false", `{"success":false,"message":"upstream down"}`, plain, true, VerdictErrorOnly},
		{"status error", `{"status":"error","code":500}`, plain, true, VerdictErrorOnly},
		{"error null is not an error", `{"error":null,"message":"done"}`, plain, true, VerdictOK},
		{"error next to data", `{"error":"partial","results":[1]}`, plain, true, VerdictOK},
		{"partial body is not checked", `{"results":`, plain, false, VerdictUncheckedSize},
		{"over the cap", `{"a":"` + strings.Repeat("x", 2000) + `"}`, plain, true, VerdictUncheckedSize},
	} {
		if got := s.checkAnswer(&pricing.Route{Name: tc.name}, tc.plan, []byte(tc.body), tc.complete); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}

	zp := answerPlan{check: true, gzip: true}
	if got := s.checkAnswer(&pricing.Route{Name: "z"}, zp, gz(`{"error":"x"}`), true); got != VerdictErrorOnly {
		t.Errorf("gzip error: %s", got)
	}
	if got := s.checkAnswer(&pricing.Route{Name: "z"}, zp, gz(`{"a":1}`), true); got != VerdictOK {
		t.Errorf("gzip ok: %s", got)
	}
	if got := s.checkAnswer(&pricing.Route{Name: "z"}, zp, gz(strings.Repeat(" ", 4096)+"{}"), true); got != VerdictUncheckedSize {
		t.Errorf("gzip bomb: %s", got)
	}
	if got := s.checkAnswer(&pricing.Route{Name: "z"}, zp, []byte("not gzip"), true); got != VerdictNotJSON {
		t.Errorf("bad gzip: %s", got)
	}
}

func TestErrorShapedExampleIsNormal(t *testing.T) {
	s := &Server{answerMax: 1024}
	r := jsonRoute("r", `{"status":"error","message":"no such name"}`)
	if got := s.checkAnswer(r, answerPlan{check: true}, []byte(`{"status":"error","message":"x"}`), true); got != VerdictOK {
		t.Fatalf("seller's normal error-shaped answer: %s", got)
	}
}

func TestPlanAnswer(t *testing.T) {
	declared := jsonRoute("d", "")
	bare := &pricing.Route{Name: "b"}
	for _, tc := range []struct {
		route *pricing.Route
		code  int
		head  bool
		ct    string
		enc   string
		check bool
		why   string
	}{
		{bare, 200, false, "application/json; charset=utf-8", "", true, ""},
		{declared, 200, true, "application/json", "", false, VerdictUncheckedType},
		{declared, 205, false, "", "", false, VerdictUncheckedType},
		{bare, 200, false, "application/problem+json", "", true, ""},
		{bare, 200, false, "text/csv", "", false, VerdictUncheckedType},
		{bare, 200, false, "", "", false, VerdictUncheckedType},
		{declared, 200, false, "", "", true, ""},
		{declared, 200, false, "text/html", "", true, ""},
		{declared, 204, false, "", "", false, VerdictUncheckedType},
		{declared, 200, false, "text/event-stream", "", false, VerdictUncheckedType},
		{declared, 200, false, "application/json", "br", false, VerdictUncheckedEnc},
		{declared, 200, false, "application/json", "gzip", true, ""},
	} {
		p := planAnswer(tc.route, tc.code, tc.head, map[string]string{"content-type": tc.ct, "content-encoding": tc.enc})
		if p.check != tc.check || p.verdict != tc.why {
			t.Errorf("%s %d %q %q: %+v", tc.route.Name, tc.code, tc.ct, tc.enc, p)
		}
	}
}

func TestShapeLearning(t *testing.T) {
	var sh shapes
	answer := func(keys ...string) map[string]json.RawMessage {
		m := map[string]json.RawMessage{}
		for _, k := range keys {
			m[k] = json.RawMessage("1")
		}
		return m
	}
	// Before enough answers are seen, nothing is missing.
	for i := 0; i < shapeMinSeen-1; i++ {
		sh.observe("r", answer("name", "taken", "domain"))
	}
	if got := sh.observe("r", answer("other")); got != VerdictOK {
		t.Fatalf("judged before learning: %s", got)
	}
	for i := 0; i < shapeWindow; i++ {
		sh.observe("r", answer("name", "taken", "domain"))
	}
	if got := sh.observe("r", answer("name")); got != VerdictMissingFields {
		t.Fatalf("two of three reliable fields missing: %s", got)
	}
	if got := sh.observe("r", answer("name", "taken")); got != VerdictOK {
		t.Fatalf("one of three missing is not refused: %s", got)
	}
	if got := sh.observe("r", answer("name", "taken", "domain", "extra")); got != VerdictOK {
		t.Fatalf("extra fields: %s", got)
	}
	// A seller who changes the answer's shape is refused for a few
	// answers, then the old fields stop counting as reliable.
	refused := 0
	for i := 0; i < shapeWindow; i++ {
		if sh.observe("r", answer("v2")) == VerdictMissingFields {
			refused++
		}
	}
	if refused == 0 || refused > 3 {
		t.Fatalf("new shape refused %d times", refused)
	}
	// Routes are learned separately, and their number is capped.
	if got := sh.observe("other", answer()); got != VerdictOK {
		t.Fatal(got)
	}
	for i := 0; i < shapeRoutes+10; i++ {
		sh.observe(fmt.Sprint("r", i), answer("a"))
	}
	if len(sh.routes) > shapeRoutes {
		t.Fatalf("%d routes tracked", len(sh.routes))
	}
}

func TestReadableEncoding(t *testing.T) {
	for in, want := range map[string]string{
		"":                        "identity",
		"gzip":                    "gzip",
		"gzip, deflate, br, zstd": "gzip",
		"br, zstd":                "identity",
		"br;q=1.0, gzip;q=0.5":    "gzip",
		"gzip;q=0, br":            "identity",
		"GZIP;Q=0.000, br":        "identity",
		"x-gzip":                  "gzip",
		"*":                       "gzip",
		"*;q=0, identity":         "identity",
		"br, identity;q=0":        "",
		"br, *;q=0":               "",
		"identity":                "identity",
	} {
		if got := readableEncoding(in); got != want {
			t.Errorf("readableEncoding(%q) = %q, want %q", in, got, want)
		}
	}
}

// Review L4: gzip padding after the member and a UTF-8 BOM are not broken
// answers.
func TestCheckAnswerGzipTrailerAndBOM(t *testing.T) {
	s := &Server{answerMax: 1024}
	r := jsonRoute("r", "")
	padded := append(gz(`{"a":1}`), 0, 0, 0, 0)
	if v := s.checkAnswer(r, answerPlan{check: true, gzip: true}, padded, true); v != VerdictOK {
		t.Errorf("gzip with trailing zeros: %s", v)
	}
	if v := s.checkAnswer(r, answerPlan{check: true}, []byte("\xef\xbb\xbf{\"a\":1}"), true); v != VerdictOK {
		t.Errorf("BOM: %s", v)
	}
}
