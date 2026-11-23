package pricing

import (
	"encoding/json"
	"testing"
)

func TestSchemaOf(t *testing.T) {
	var v any
	json.Unmarshal([]byte(`{"n":1,"f":1.5,"s":"x","b":true,"z":null,"a":[{"k":"v"}],"e":[]}`), &v)
	got, _ := json.Marshal(SchemaOf(v))
	want := `{"properties":{"a":{"items":{"properties":{"k":{"type":"string"}},"required":["k"],"type":"object"},"type":"array"},"b":{"type":"boolean"},"e":{"type":"array"},"f":{"type":"number"},"n":{"type":"integer"},"s":{"type":"string"},"z":{}},"required":["a","b","e","f","n","s","z"],"type":"object"}`
	if string(got) != want {
		t.Fatalf("schema:\n%s\nwant\n%s", got, want)
	}
}
