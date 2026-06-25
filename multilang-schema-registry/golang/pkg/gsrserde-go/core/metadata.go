package gsrserde

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// putSchemaVersionMetadataBatch writes each (k,v) entry to Glue under
// schemaVersionId via PutSchemaVersionMetadata. Mirrors Java
// AWSSchemaRegistryClient.java:418-429 (which uses parallelStream + warn-on-
// failure per-entry). The Go translation uses a SEQUENTIAL for-loop and logs
// per-entry failures via stdlib log.Printf with the "gsr: " prefix; it does
// NOT abort the batch (Java parity at line 425 — partial-write is a logged-
// only condition, INV-6).
//
// The combined map ALWAYS includes the (TransportMetadataKey, transportName)
// entry — including when transportName == "" — to match Java
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/GlueSchemaRegistrySerializationFacade.java:90-95
// (unconditional `put` before the configured-metadata merge). Pinned by
// AC-9 / AC-9b / INV-7 / C-12. Java's ordering at
// AWSSchemaRegistryClient.java:419-422 is
// `metadata.put(transport); metadata.putAll(configured)` — configured wins
// on collision (AC-4-collision).
//
// Concurrency choice: SEQUENTIAL for-loop. Rationale (spec §3.4(d)):
// (a) typical cardinality is 1–10 entries; (b) deterministic ordering
// keeps trace output clean; (c) Java's parallelStream is a JVM-side perf
// detail, not a behavioral contract. The sequential-only contract is
// pinned by INV-9 / C-11 — see the corresponding spec section for the
// list of primitives this helper MUST NOT use.
//
// Logger choice (OQ-2): stdlib `log.Printf` with the "gsr: " prefix. No new
// dependency (no slog, no logrus). Pinned by INV-8 / C-21.
//
// Returns nil on partial success (one or more entries succeeded). Returns
// an aggregated error wrapping ErrGSR only when EVERY entry failed — that
// signal is informational; callers (the encoder) intentionally drop it per
// INV-6. AlreadyExistsException on a metadata key is treated as success
// (Java's swallow-and-continue at line 425).
func (s *GsrEncoder) putSchemaVersionMetadataBatch(
	ctx context.Context,
	schemaVersionId, transportName string,
) error {
	// Merge order matters: start with the transport entry, then overlay
	// the customer's configured metadata. When the customer configures
	// `metadata.x-amz-meta-transport=foo`, this produces len(s.metadata)
	// total entries (not len+1) and the transport-key value is the
	// customer's `foo`, not the per-call `transportName` — Java parity at
	// AWSSchemaRegistryClient.java:419-422 (AC-4-collision).
	merged := make(map[string]string, len(s.metadata)+1)
	merged[TransportMetadataKey] = transportName
	for k, v := range s.metadata {
		merged[k] = v
	}

	if len(merged) == 0 {
		// Defensive guard. With the always-injected transport entry above
		// the merged map can never reach zero entries in normal flow, but
		// the explicit early return keeps the contract readable.
		return nil
	}

	total := len(merged)
	failed := 0
	for k, v := range merged {
		// Copy loop variables into locals so &k / &v point at stable
		// addresses (Glue's *string inputs require pointers; using the
		// loop variable's address would alias across iterations and
		// race against future Go versions that reuse the loop slot).
		key := k
		val := v
		_, err := s.client.PutSchemaVersionMetadata(ctx, &glue.PutSchemaVersionMetadataInput{
			SchemaVersionId: &schemaVersionId,
			MetadataKeyValue: &types.MetadataKeyValuePair{
				MetadataKey:   &key,
				MetadataValue: &val,
			},
		})
		if err != nil {
			// Java parity: AWSSchemaRegistryClient.java:425 — log a WARN
			// and continue. Go stdlib log has no WARN level; the "gsr: "
			// prefix marks GSR-originated warnings.
			log.Printf("gsr: put schema version metadata key=%s value=%s: %v", key, val, err)
			failed++
		}
	}

	if failed == total {
		// All entries failed: surface an aggregated error chained through
		// ErrGSR. The encoder MUST NOT propagate this to Encode's return —
		// INV-6 / C-13 lock the metadata-write-non-fatal contract. The
		// signal exists for future telemetry hooks and for unit tests
		// asserting the all-failure aggregation shape.
		return fmt.Errorf("%w: put schema version metadata: all %d entries failed for schemaVersionId=%s",
			ErrGSR, total, schemaVersionId)
	}
	return nil
}
