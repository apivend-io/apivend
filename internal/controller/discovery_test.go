package controller

import (
	"encoding/json"
	"reflect"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	x402v1 "apivend.io/apivend/api/v1alpha1"
)

func nameCheckDiscovery(enabled bool) *x402v1.Discovery {
	return &x402v1.Discovery{
		Enabled:     enabled,
		ServiceName: "Name check",
		Tags:        []string{"names", "domains"},
		QueryParams: []x402v1.Param{
			{Name: "name", Description: "The name to check", Required: true, Example: "larkspindle"},
			{Name: "tlds", Description: "Comma-separated TLDs", Example: "com,dev"},
		},
		Output: &x402v1.Output{Example: &apiextensionsv1.JSON{Raw: []byte(`{"name":"larkspindle","complete":true}`)}},
	}
}

func TestBazaarFor(t *testing.T) {
	raw, err := BazaarFor(nameCheckDiscovery(true))
	if err != nil {
		t.Fatal(err)
	}
	var ext struct {
		Info struct {
			Input  map[string]any `json:"input"`
			Output map[string]any `json:"output"`
		} `json:"info"`
		Schema map[string]any `json:"schema"`
	}
	if err := json.Unmarshal(raw, &ext); err != nil {
		t.Fatal(err)
	}
	in := ext.Info.Input
	if in["type"] != "http" || in["method"] != "GET" || !reflect.DeepEqual(in["queryParams"], map[string]any{"name": "larkspindle", "tlds": "com,dev"}) {
		t.Fatalf("input = %v", in)
	}
	if ext.Info.Output["type"] != "json" || ext.Info.Output["example"].(map[string]any)["complete"] != true {
		t.Fatalf("output = %v", ext.Info.Output)
	}
	props := ext.Schema["properties"].(map[string]any)["input"].(map[string]any)
	qp := props["properties"].(map[string]any)["queryParams"].(map[string]any)
	if !reflect.DeepEqual(qp["required"], []any{"name"}) || !reflect.DeepEqual(props["required"], []any{"type", "method", "queryParams"}) {
		t.Fatalf("schema input = %v", props)
	}
	if ext.Schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" || !reflect.DeepEqual(ext.Schema["required"], []any{"input"}) {
		t.Fatalf("schema = %v", ext.Schema)
	}

	post := &x402v1.Discovery{Method: "POST", Body: &apiextensionsv1.JSON{Raw: []byte(`{"q":"x"}`)}}
	raw, err = BazaarFor(post)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &ext); err != nil || ext.Info.Input["bodyType"] != "json" || ext.Info.Input["body"].(map[string]any)["q"] != "x" {
		t.Fatalf("post input = %v (%v)", ext.Info.Input, err)
	}

	for name, d := range map[string]*x402v1.Discovery{
		"required without example": {QueryParams: []x402v1.Param{{Name: "q", Required: true}}},
		"duplicate parameter":      {QueryParams: []x402v1.Param{{Name: "q", Example: "a"}, {Name: "q", Example: "b"}}},
		"GET with a body":          {Body: &apiextensionsv1.JSON{Raw: []byte(`{}`)}},
		"bad body":                 {Method: "POST", Body: &apiextensionsv1.JSON{Raw: []byte(`{`)}},
		"bad output":               {Output: &x402v1.Output{Example: &apiextensionsv1.JSON{Raw: []byte(`{`)}}},
	} {
		if _, err := BazaarFor(d); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDiscoveryReachesTheFilter(t *testing.T) {
	listed := policy("prod", "listed", "search", t0)
	listed.Spec.Description = "Is a name taken on npm, PyPI and as a domain"
	listed.Spec.Discovery = nameCheckDiscovery(true)
	quiet := policy("prod", "quiet", "other", t0)
	quiet.Spec.Description = "Same, unpublished"
	quiet.Spec.Discovery = nameCheckDiscovery(false)
	bad := policy("prod", "bad", "third", t0)
	bad.Spec.Description = "Bad discovery"
	bad.Spec.Discovery = &x402v1.Discovery{QueryParams: []x402v1.Param{{Name: "q", Required: true}}}
	undescribed := policy("prod", "undescribed", "fourth", t0)
	undescribed.Spec.Discovery = nameCheckDiscovery(true) // no description

	w := newWorld(t, append(base(), httpRoute("prod", "other"), httpRoute("prod", "third"), httpRoute("prod", "fourth"), listed, quiet, bad, undescribed)...)
	w.reconcile()
	w.accepted("prod", "listed", true, "Accepted")
	w.accepted("prod", "quiet", true, "Accepted")
	w.accepted("prod", "bad", false, "Invalid")
	w.accepted("prod", "undescribed", false, "Invalid")

	// Through the ConfigMap and the filter's own parser.
	table := w.table()
	l := table.Match("", "/", "httproute/prod/search/rule/0").Discovery
	q := table.Match("", "/", "httproute/prod/other/rule/0").Discovery
	if l == nil || !l.Publish || l.ServiceName != "Name check" || len(l.Bazaar) == 0 {
		t.Fatalf("listed = %+v", l)
	}
	if q == nil || q.Publish {
		t.Fatalf("quiet = %+v", q)
	}
	// The same JSON (the YAML price table may reorder object keys).
	want, _ := BazaarFor(nameCheckDiscovery(true))
	var got, exp any
	json.Unmarshal(l.Bazaar, &got)
	json.Unmarshal(want, &exp)
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("bazaar through the price table:\n%s\nwant\n%s", l.Bazaar, want)
	}
}
