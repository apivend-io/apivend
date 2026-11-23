package pricing

import "slices"

// SchemaOf infers a JSON Schema from an example value: types, object
// properties (all present ones required) and array items from the first
// element. Nesting is cut at 6 levels.
func SchemaOf(v any) map[string]any { return schemaOf(v, 0) }

func schemaOf(v any, depth int) map[string]any {
	if depth >= 6 {
		return map[string]any{}
	}
	switch x := v.(type) {
	case map[string]any:
		props := map[string]any{}
		req := make([]string, 0, len(x))
		for k, e := range x {
			props[k] = schemaOf(e, depth+1)
			req = append(req, k)
		}
		slices.Sort(req)
		s := map[string]any{"type": "object", "properties": props}
		if len(req) > 0 {
			s["required"] = req
		}
		return s
	case []any:
		s := map[string]any{"type": "array"}
		if len(x) > 0 {
			s["items"] = schemaOf(x[0], depth+1)
		}
		return s
	case string:
		return map[string]any{"type": "string"}
	case float64:
		if x == float64(int64(x)) {
			return map[string]any{"type": "integer"}
		}
		return map[string]any{"type": "number"}
	case bool:
		return map[string]any{"type": "boolean"}
	default:
		return map[string]any{}
	}
}
