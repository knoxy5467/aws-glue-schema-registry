//go:build !integration

// Fallback for the non-integration build: container mode is not available
// because we don't pull testcontainers-go into the unit-tier graph. New()
// dispatches to startContainerStub which returns a clear error.

package javasidecar

import "context"

func startContainer(_ context.Context, _ Options) (*Sidecar, error) {
	return startContainerStub()
}
