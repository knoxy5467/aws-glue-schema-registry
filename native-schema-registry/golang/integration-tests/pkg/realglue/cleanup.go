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
	// seenSchema and seenRegistry dedupe re-tracks so a test that
	// registers the same name twice doesn't double-delete.
	seenSchema   map[string]struct{}
	seenRegistry map[string]struct{}
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
	c.mu.Unlock()

	var errs []error
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

func isNotFound(err error) bool {
	var enf *types.EntityNotFoundException
	return errors.As(err, &enf)
}
