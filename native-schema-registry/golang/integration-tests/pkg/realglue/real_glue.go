// Package realglue provides a real-AWS-Glue-backed implementation of
// gsrcore.GlueClient for the Tier-2 integration tests.
//
// Phase 4 ran every §5.3 scenario against integration-tests/pkg/fakeglue
// — no AWS billing, no service-side validation. Phase 4.7 (see
// PHASE-4.7-PLAN.md) adds this package so scenarios can ALSO run
// against real Glue when the user opts in via:
//
//	GSR_GLUE=real AWS_INTEGRATION=1 make test-integ-real
//
// Default behavior is unchanged: GSR_GLUE unset or "fake" returns
// fakeglue, no AWS credentials needed. The selector lives in
// tests/glue_selector_test.go.
package realglue

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// DefaultRegion is the fall-back region used when neither the
// option nor AWS_REGION is set. Anchored to plan §6.3 (the existing
// Java / C# canary account runs in us-east-2).
const DefaultRegion = "us-east-2"

// Options bundles every knob New accepts. Options is exported only
// for the test seam; production callers should use the With* option
// functions.
type Options struct {
	region   string
	profile  string
	endpoint string
	// awsConfig, when non-nil, short-circuits config.LoadDefaultConfig
	// — used by the unit tests so they don't reach for the credential
	// chain on the dev host.
	awsConfig *aws.Config
}

// Option is the functional option type the package exposes.
type Option func(*Options)

// WithRegion overrides the region. If unset, New reads AWS_REGION,
// falling back to DefaultRegion.
func WithRegion(region string) Option {
	return func(o *Options) { o.region = region }
}

// WithProfile passes a shared-config profile name to
// config.LoadDefaultConfig.
func WithProfile(profile string) Option {
	return func(o *Options) { o.profile = profile }
}

// WithEndpoint sets the Glue client's BaseEndpoint. Used in dev
// environments that route Glue traffic through a regional VPC
// endpoint or a localstack-ultimate-style override.
func WithEndpoint(endpoint string) Option {
	return func(o *Options) { o.endpoint = endpoint }
}

// WithAWSConfig is the test-only seam: callers inject a pre-built
// aws.Config so New skips config.LoadDefaultConfig and the
// Credentials.Retrieve probe. NOT for production use.
func WithAWSConfig(cfg aws.Config) Option {
	return func(o *Options) { o.awsConfig = &cfg }
}

// Real is the real-AWS-backed GlueClient. It embeds *glue.Client so
// every method on the gsrcore.GlueClient interface is satisfied by
// the SDK directly — no shim layer that could drift.
type Real struct {
	*glue.Client
	// Region is captured so the runbook / banner can display it.
	Region string
}

// New builds a *Real client. Returns an error if config resolution
// or credential retrieval fails. Credential retrieval is performed
// eagerly so a missing-creds run fails fast — at construction —
// rather than at the first scenario's encode call.
func New(ctx context.Context, opts ...Option) (*Real, error) {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	// Code-review finding #13 (Phase 4.7): WithProfile is meaningful only
	// when New resolves credentials via LoadDefaultConfig. If the caller
	// also passes WithAWSConfig, the profile would be silently ignored.
	// Erroring is preferable to surprising downstream debugging.
	if o.awsConfig != nil && o.profile != "" {
		return nil, fmt.Errorf("realglue: WithProfile is incompatible with WithAWSConfig (the injected config carries its own credentials)")
	}

	// Region precedence (uniform across the injected-config and
	// LoadDefaultConfig branches):
	//   1. WithRegion option (explicit override)
	//   2. injected aws.Config.Region (only when WithAWSConfig was used)
	//   3. AWS_REGION env var
	//   4. DefaultRegion (us-east-2; plan §6.3 anchor)
	region := o.region
	if region == "" && o.awsConfig != nil {
		region = o.awsConfig.Region
	}
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = DefaultRegion
	}

	var cfg aws.Config
	if o.awsConfig != nil {
		cfg = *o.awsConfig
		cfg.Region = region
	} else {
		loadOpts := []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(region),
		}
		if o.profile != "" {
			loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(o.profile))
		}
		var err error
		cfg, err = awsconfig.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("realglue: load AWS config: %w", err)
		}
		// Eager credential probe: surface missing chain at construction.
		if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
			return nil, fmt.Errorf("realglue: resolve AWS credentials: %w", err)
		}
	}

	glueOpts := []func(*glue.Options){}
	if o.endpoint != "" {
		ep := o.endpoint
		glueOpts = append(glueOpts, func(go_ *glue.Options) {
			go_.BaseEndpoint = aws.String(ep)
		})
	}

	client := glue.NewFromConfig(cfg, glueOpts...)
	return &Real{Client: client, Region: region}, nil
}

// NewCleanup returns a Cleanup catalog bound to this Real client. The
// selector calls this and wires Run into t.Cleanup so teardown fires
// even on t.Fail.
func (r *Real) NewCleanup() *Cleanup {
	return newCleanup(r.Client)
}
