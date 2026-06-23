//go:build integration

// Container-mode startup lives behind the integration build tag because
// testcontainers-go pulls a heavy transitive graph (Docker client, network
// plumbing) we don't want in the unit-tier binary.

package javasidecar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultContainerImage = "gsr-go-it-java-sidecar:latest"
	defaultContainerPort  = 8080
)

func startContainer(ctx context.Context, opts Options) (*Sidecar, error) {
	image := opts.Image
	if image == "" {
		image = defaultContainerImage
	}
	port := opts.ContainerPort
	if port == 0 {
		port = defaultContainerPort
	}
	natPortStr := fmt.Sprintf("%d/tcp", port)

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{natPortStr},
		WaitingFor: wait.ForHTTP("/health").
			WithPort(natPortStr).
			WithStartupTimeout(opts.StartTimeout),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("javasidecar: container start: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, fmt.Errorf("javasidecar: container host: %w", err)
	}
	mapped, err := c.MappedPort(ctx, natPortStr)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, fmt.Errorf("javasidecar: container mapped port: %w", err)
	}

	stop := func(stopCtx context.Context) error {
		if err := c.Terminate(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}

	sc := &Sidecar{
		baseURL: fmt.Sprintf("http://%s:%s", host, mapped.Port()),
		client:  opts.HTTPClient,
		mode:    ModeContainer,
		stop:    stop,
	}
	// WaitingFor.ForHTTP already polled /health; one extra ping ensures the
	// container-network -> host port mapping is also live before the test
	// fires its first request.
	if err := waitHealthy(ctx, sc, 5*time.Second); err != nil {
		_ = sc.Stop(context.Background())
		return nil, fmt.Errorf("javasidecar: container never became healthy: %w", err)
	}
	return sc, nil
}
