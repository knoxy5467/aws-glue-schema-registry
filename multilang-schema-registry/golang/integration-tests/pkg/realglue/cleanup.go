package realglue

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// cleanupClient is the narrow seam between Cleanup.Run and the AWS
// Glue v2 SDK. *glue.Client satisfies it; the unit tests substitute a
// recorder stub.
type cleanupClient interface {
	DeleteSchema(ctx context.Context, params *glue.DeleteSchemaInput, optFns ...func(*glue.Options)) (*glue.DeleteSchemaOutput, error)
	DeleteRegistry(ctx context.Context, params *glue.DeleteRegistryInput, optFns ...func(*glue.Options)) (*glue.DeleteRegistryOutput, error)
	ListSchemas(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error)
}

// Cleanup catalogs the registries and schemas a test creates so that
// Cleanup.Run can delete them — schemas before registries, both in
// reverse insertion order — when the test completes.
//
// Cleanup is safe to use from multiple goroutines; TrackSchema /
// TrackRegistry take the mutex.
//
// Zero value is NOT usable: callers must build via newCleanup so the
// client seam is wired. The selector wires it; tests use
// newCleanupForTest.
type Cleanup struct {
	mu         sync.Mutex
	client     cleanupClient
	schemas    []schemaRef
	registries []string
	// prefixes are (registry, prefix) tuples — Run will list-and-delete
	// every schema under registry whose name starts with prefix. Use
	// this for tests where the actual registered name is computed from
	// downstream code (e.g. a SchemaNameStrategy) and the test can only
	// guarantee a prefix.
	prefixes []prefixRef
	// seenSchema and seenRegistry dedupe re-tracks so a test that
	// registers the same name twice doesn't double-delete.
	seenSchema   map[string]struct{}
	seenRegistry map[string]struct{}
	seenPrefix   map[string]struct{}
}

type prefixRef struct {
	Registry string
	Prefix   string
}

type schemaRef struct {
	Registry string
	Name     string
}

func newCleanup(client cleanupClient) *Cleanup {
	return &Cleanup{
		client:       client,
		seenSchema:   map[string]struct{}{},
		seenRegistry: map[string]struct{}{},
		seenPrefix:   map[string]struct{}{},
	}
}

// newCleanupForTest is the test-only constructor. Exported via a
// package-internal function so external tests can't accidentally
// construct one without going through the selector.
func newCleanupForTest(client cleanupClient) *Cleanup {
	return newCleanup(client)
}

// TrackSchema records a schema (registry, name) for later deletion.
// Tests call this immediately after a successful Encode / CreateSchema.
func (c *Cleanup) TrackSchema(registry, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := registry + "/" + name
	if _, ok := c.seenSchema[key]; ok {
		return
	}
	c.seenSchema[key] = struct{}{}
	c.schemas = append(c.schemas, schemaRef{Registry: registry, Name: name})
}

// TrackSchemaPrefix records a (registry, prefix) tuple. At Run time,
// every schema under registry whose name starts with prefix is deleted.
//
// Use this when the test cannot pin the exact registered schema name —
// e.g. the producer goes through a SchemaNameStrategy that derives the
// name from topic / payload, and the test only knows the prefix it gave
// its topic. Prefer TrackSchema when the exact name is known: a typo'd
// prefix here can silently scan-and-delete unrelated schemas.
func (c *Cleanup) TrackSchemaPrefix(registry, prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := registry + "/" + prefix
	if _, ok := c.seenPrefix[key]; ok {
		return
	}
	c.seenPrefix[key] = struct{}{}
	c.prefixes = append(c.prefixes, prefixRef{Registry: registry, Prefix: prefix})
}

// TrackRegistry records a registry name for later deletion.
func (c *Cleanup) TrackRegistry(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seenRegistry[name]; ok {
		return
	}
	c.seenRegistry[name] = struct{}{}
	c.registries = append(c.registries, name)
}

// Run deletes every tracked schema (reverse order) then every tracked
// registry (reverse order). Errors from individual deletes are joined
// with errors.Join so a single mid-run failure does not skip the
// remaining deletes — important when a test leaves N resources and
// only one of them is in a bad state.
//
// EntityNotFoundException is suppressed: a test that already
// cleaned up directly, or a real-Glue race where another caller
// deleted the same name, should not turn the cleanup red.
func (c *Cleanup) Run(ctx context.Context) error {
	c.mu.Lock()
	schemas := append([]schemaRef(nil), c.schemas...)
	registries := append([]string(nil), c.registries...)
	prefixes := append([]prefixRef(nil), c.prefixes...)
	c.mu.Unlock()

	var errs []error
	// Resolve every prefix into concrete schema names and merge into the
	// schemas list before deleting. Done first so prefix-discovered
	// schemas inherit the same EntityNotFound suppression + reverse-order
	// guarantees as explicitly tracked schemas.
	for _, p := range prefixes {
		found, err := c.listSchemasByPrefix(ctx, p.Registry, p.Prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("ListSchemas prefix=%s/%s: %w", p.Registry, p.Prefix, err))
			continue
		}
		for _, name := range found {
			key := p.Registry + "/" + name
			if _, ok := c.seenSchema[key]; ok {
				continue
			}
			schemas = append(schemas, schemaRef{Registry: p.Registry, Name: name})
		}
	}

	for i := len(schemas) - 1; i >= 0; i-- {
		s := schemas[i]
		_, err := c.client.DeleteSchema(ctx, &glue.DeleteSchemaInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(s.Registry),
				SchemaName:   aws.String(s.Name),
			},
		})
		if err != nil && !isNotFound(err) {
			errs = append(errs, fmt.Errorf("DeleteSchema %s/%s: %w", s.Registry, s.Name, err))
		}
	}
	for i := len(registries) - 1; i >= 0; i-- {
		name := registries[i]
		_, err := c.client.DeleteRegistry(ctx, &glue.DeleteRegistryInput{
			RegistryId: &types.RegistryId{RegistryName: aws.String(name)},
		})
		if err != nil && !isNotFound(err) {
			errs = append(errs, fmt.Errorf("DeleteRegistry %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// listSchemasByPrefix walks ListSchemas for the given registry,
// paginating until empty, and returns every schema name whose name
// starts with prefix. Glue's ListSchemas API doesn't support a
// server-side name filter so the filter runs client-side.
func (c *Cleanup) listSchemasByPrefix(ctx context.Context, registry, prefix string) ([]string, error) {
	var names []string
	var nextToken *string
	for {
		out, err := c.client.ListSchemas(ctx, &glue.ListSchemasInput{
			RegistryId: &types.RegistryId{RegistryName: aws.String(registry)},
			NextToken:  nextToken,
		})
		if err != nil {
			return nil, err
		}
		for _, s := range out.Schemas {
			if s.SchemaName == nil {
				continue
			}
			n := *s.SchemaName
			if len(prefix) == 0 || (len(n) >= len(prefix) && n[:len(prefix)] == prefix) {
				names = append(names, n)
			}
		}
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}
	return names, nil
}

func isNotFound(err error) bool {
	var enf *types.EntityNotFoundException
	return errors.As(err, &enf)
}
