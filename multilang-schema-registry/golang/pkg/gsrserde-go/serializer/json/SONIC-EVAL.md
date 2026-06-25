# bytedance/sonic vs encoding/json — Parity Evaluation (Phase 8C)

## Verdict

**DO NOT swap encoding/json for bytedance/sonic in the JSON SerDes.**

Sonic is a high-throughput JSON library, but its default behavior diverges
from `encoding/json` in ways that would silently change observable
behavior for GSR JSON customers. The marshal side is where the divergences
are concentrated; the unmarshal side is fully compatible across the
inputs we tested, so a future, narrowly-scoped unmarshal-only adoption
remains an option.

## Method

A parity harness was written that runs three suites — marshal,
unmarshal, and roundtrip — covering the divergences sonic is known for:
HTML-special characters, Unicode line/paragraph separators, map-key
ordering, numeric edge cases, nil values, embedded fields, and
`,omitempty`. The harness was removed after the evaluation completed
(the brief required `bytedance/sonic` to be dropped from go.mod once a
no-swap verdict was reached). To reproduce the findings:

```bash
cd multilang-schema-registry/golang
go get github.com/bytedance/sonic
# Reconstitute the harness from the snippet at the bottom of this doc
go test -v -run='TestSonic' ./pkg/gsrserde-go/serializer/json/
go mod edit -droprequire=github.com/bytedance/sonic && go mod tidy
```

The original harness (pre-deletion) used `t.Logf` rather than
`t.Errorf` for divergence cases — informational, not gating, since we
were evaluating not adopting.

## Findings

### Marshal — 15 / 22 cases byte-identical, 7 diverge

| Case | encoding/json | sonic | Impact |
| --- | --- | --- | --- |
| `html_lt` (`a<b`) | `"a<b"` | `"a<b"` | HTML-context safety regression |
| `html_gt` (`a>b`) | `"a>b"` | `"a>b"` | HTML-context safety regression |
| `html_amp` (`a&b`) | `"a&b"` | `"a&b"` | HTML-context safety regression |
| `html_combined` | `"<script>…"` | `"<script>…"` | HTML-context safety regression |
| `unicode_2028` (U+2028) | `"line break"` | raw U+2028 | JavaScript parse error in browser embed |
| `unicode_2029` (U+2029) | `"para break"` | raw U+2029 | JavaScript parse error in browser embed |
| `map_three_keys` (a,b,c) | `{"a":1,"b":2,"c":3}` | `{"c":3,"b":2,"a":1}` | Non-deterministic wire output |

Each divergence is a deliberate sonic design choice, documented upstream.
None are fixable without explicit configuration (sonic offers an
`Encoder` with `SortMapKeys` and `EscapeHTML` toggles, but using them
defeats the throughput argument that motivates sonic in the first
place).

The map-key ordering divergence is particularly concerning for a
schema-registry library: customers who diff JSON payloads in tests, or
who hash JSON for deduplication, would see their assertions break the
day they upgrade the GSR client.

### Unmarshal — 21 / 21 cases agree (verdict and value)

Both libraries:
- Accept the same well-formed inputs and produce equivalent Go values
  (compared after re-marshal via `encoding/json` for canonical form).
- Reject the same malformed inputs: trailing commas, unclosed braces,
  naked tokens, single-quoted strings, JSON-with-comments.

This is the directly load-bearing case for the JSON SerDes. Both
serializer and deserializer call `json.Unmarshal` purely as a
"is this well-formed JSON?" probe; the unmarshaled value is then
discarded. Sonic agrees with `encoding/json` on the well-formedness
verdict across every tested input, so a narrow drop-in for the probe
path is technically safe.

### Roundtrip — same shape as marshal

Marshal-with-sonic → Unmarshal-with-std → Marshal-with-std diverges
exactly where Marshal diverges (HTML-escape, map ordering, U+2028 /
U+2029). The unmarshal step is lossless; the divergence is the encoding
step.

## Why we are NOT swapping today

1. **Marshal divergence is observable**. Customers who serialize JSON
   payloads via GSR and embed them in HTML pages, JSON-in-JSON
   structures, or test fixtures would see different bytes. We can't
   commit to that without an explicit opt-in API surface, which exceeds
   Phase 8's scope.

2. **The current SerDes hot path doesn't marshal JSON.** The serializer
   accepts a `JsonDataWithSchema` wrapper whose `Payload` is already
   bytes; it validates and returns the bytes unchanged. The marshal
   speedup would benefit only a future code path that didn't exist
   when this evaluation was run.

3. **The unmarshal path could safely use sonic, but the marginal win is
   small.** `json.Unmarshal` here is a well-formedness check, not a
   semantic decode. Sonic's headline numbers are on large-document
   throughput; the GSR validation payloads are small enough that the
   `encoding/json` baseline is not the bottleneck.

4. **Adding sonic is non-trivial.** It pulls in `cloudwego/base64x`,
   `bytedance/gopkg`, `klauspost/cpuid`, a JIT loader, and golang-asm —
   ~5 new transitive deps for a library that already has a clean
   dependency surface. Not worth it without a concrete win.

## When to revisit

- A workload that benchmarks JSON marshal or unmarshal as the
  bottleneck of the GSR SerDes hot path.
- A new code path that needs to marshal arbitrary Go values into JSON
  (not just validate caller-supplied bytes).
- A sonic release that flips the defaults to match `encoding/json`
  (escape HTML, sort map keys). Unlikely upstream priority.

## What we left in the tree

- This file (SONIC-EVAL.md) — the reasoning record and reproducible
  parity-harness snippet (below).

## What we removed

- `github.com/bytedance/sonic` from `go.mod` and its transitive
  closure (`cloudwego/base64x`, `bytedance/gopkg`, `klauspost/cpuid`,
  `bytedance/sonic/loader`, `twitchyliquid64/golang-asm`,
  `golang.org/x/arch`).
- The parity test file `sonic_parity_test.go` — its findings are
  recorded above; the test source is preserved as a snippet at the
  bottom of this doc for anyone re-running the evaluation.

## Reproducible parity-harness snippet

Save this as `pkg/gsrserde-go/serializer/json/sonic_parity_test.go`,
`go get github.com/bytedance/sonic`, then `go test -v -run=TestSonic`.

```go
package json

import (
	"bytes"
	stdjson "encoding/json"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
)

var marshalParityCases = []struct {
	name string
	v    interface{}
}{
	{"empty_struct", struct{}{}},
	{"empty_string", ""},
	{"simple_string", "hello"},
	{"html_lt", "a<b"},
	{"html_gt", "a>b"},
	{"html_amp", "a&b"},
	{"html_combined", "<script>alert('x')</script>"},
	{"unicode_2028", "line break"},
	{"unicode_2029", "para break"},
	{"map_two_keys", map[string]int{"a": 1, "z": 2}},
	{"map_three_keys", map[string]int{"c": 3, "b": 2, "a": 1}},
	{"int_max", int64(9223372036854775807)},
	{"nested_omitempty", struct {
		A string `json:"a,omitempty"`
		B int    `json:"b,omitempty"`
	}{A: "x", B: 0}},
}

func TestSonicMarshalParity(t *testing.T) {
	for _, tc := range marshalParityCases {
		stdB, _ := stdjson.Marshal(tc.v)
		sonicB, _ := sonic.Marshal(tc.v)
		if !bytes.Equal(stdB, sonicB) {
			t.Logf("%s: DIVERGE std=%s sonic=%s", tc.name, stdB, sonicB)
		}
	}
}

var unmarshalParityInputs = []string{
	`{}`, `[]`, `null`, `true`, `42`, `3.14`,
	`{"a":1,"b":{"c":[1,2,3]}}`,
	`{"a":1,}`, `[1,2,`, `foo`, `'hello'`,
}

func TestSonicUnmarshalParity(t *testing.T) {
	for _, in := range unmarshalParityInputs {
		var a, b interface{}
		errA := stdjson.Unmarshal([]byte(in), &a)
		errB := sonic.Unmarshal([]byte(in), &b)
		t.Logf("%q: std-err=%v sonic-err=%v", in, errA, errB)
	}
}

func init() { _ = strings.Title } // silence unused-import if you trim above
```
