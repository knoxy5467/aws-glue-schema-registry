// Tier-1 retry-count assertion test for §5.3 item 24 (Phase 4.12).
//
// The pre-existing TestEncoder_Throttling_SurfacesAsErrGSRChain
// (glue_negatives_test.go) only proves that a ThrottlingException
// returned by the GlueClient interface propagates to ErrGSR. It does
// NOT exercise the SDK's middleware stack — the mock short-circuits at
// the interface boundary, so the standard retryer never runs.
//
// This file fills that gap. It builds a real glue.NewFromConfig
// client whose HTTP transport always returns a smithy-shaped
// ThrottlingException, attaches an attemptCounter middleware INSIDE
// the retry loop, wires the resulting *glue.Client into a GsrEncoder
// via the test seam, calls Encode, and asserts:
//
//  1. The encoder returns a non-nil error.
//  2. The error chain reaches a smithy.APIError with
//     ErrorCode() == "ThrottlingException".
//  3. attemptCounter.n.Load() == MaxAttempts (3) — proving the SDK's
//     standard retryer fired the full retry budget.
//
// Per spec §2.4 + §5.1 + §11 round-2 resolution:
//   - The retry middleware is at the Finalize step in the smithy-go
//     pipeline and re-invokes the rest of the handler chain on each
//     attempt.
//   - The counter MUST be attached at the Finalize step using
//     stack.Finalize.Add(counter.middleware(), middleware.After) —
//     middleware.After is the RelativePosition constant, which
//     appends after the existing entries (including "Retry"), so the
//     counter runs once per attempt INSIDE the retry loop.
//   - Initialize / Build / Send all run outside the retry loop and
//     would read 1 regardless of attempt count.
//
// The Glue v2 SDK has no concrete *types.ThrottlingException struct;
// throttling surfaces as *smithy.GenericAPIError. Assertion is
// therefore against the smithy.APIError INTERFACE, not a concrete
// typed struct (matches the existing newThrottlingError helper in
// glue_negatives_test.go and the negative_test.go::throttlingError
// helper called out in spec §11).

package gsrserde

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
)

// attemptCounter is a Finalize-step middleware that increments an
// atomic counter on each invocation. Because the SDK's standard
// retry middleware (also at Finalize) re-invokes the rest of the
// handler chain on every attempt, attaching this counter AFTER the
// retry middleware lets it observe each attempt — including the
// original — exactly once.
type attemptCounter struct{ n atomic.Int32 }

// middleware returns a FinalizeMiddleware that bumps the counter and
// delegates to the next handler. The middleware ID
// "phase412/attemptCounter" is unique within the stack so the SDK's
// duplicate-ID guard does not reject it.
func (a *attemptCounter) middleware() middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"phase412/attemptCounter",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			a.n.Add(1)
			return next.HandleFinalize(ctx, in)
		},
	)
}

// alwaysThrottleTransport is an http.RoundTripper that returns an
// HTTP 400 ThrottlingException body for every request. The body is
// shaped like an AWS JSON 1.1 error response and carries the
// X-Amzn-Errortype header smithy uses to decode the error code, so
// the SDK surfaces the response as *smithy.GenericAPIError with
// Code="ThrottlingException" — the same shape the existing
// newThrottlingError() helper builds by hand.
type alwaysThrottleTransport struct{}

func (alwaysThrottleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	const body = `{"__type":"ThrottlingException","message":"Rate exceeded"}`
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Header: http.Header{
			"Content-Type":     []string{"application/x-amz-json-1.1"},
			"X-Amzn-Errortype": []string{"ThrottlingException"},
		},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: req,
	}, nil
}

// TestEncoder_Throttling_AssertsRetryCount pins the §5.3 item 24
// contract: the SDK's standard retryer, configured to MaxAttempts=3,
// runs the full retry budget when throttled, and the encoder
// ultimately surfaces a smithy.APIError with code
// "ThrottlingException".
//
// Hermetic — no AWS calls. The fake transport intercepts every
// request before any real auth or networking happens, so the static
// credentials below are placeholders that never leave the process.
func TestEncoder_Throttling_AssertsRetryCount(t *testing.T) {
	counter := &attemptCounter{}

	// Build a minimal aws.Config sufficient for glue.NewFromConfig
	// to construct a client. The fake HTTP transport intercepts
	// every request, so credentials and region are never used on
	// the wire — they merely satisfy the SDK's signer setup.
	cfg := aws.Config{
		Region:      "us-west-2",
		Credentials: credentials.NewStaticCredentialsProvider("AKIA-TEST", "SECRET-TEST", ""),
		HTTPClient: &http.Client{
			Transport: alwaysThrottleTransport{},
		},
	}

	// Build the Glue client with:
	//   - explicit retry.NewStandard(MaxAttempts=3) so the test
	//     survives any future SDK-default drift away from 3
	//     (spec §3.7 step 2, round-2 fix).
	//   - the attemptCounter wired into APIOptions at the Finalize
	//     step using stack.Finalize.Add(mw, middleware.After). The
	//     "Retry" middleware is already in Finalize by the time
	//     APIOptions runs, and middleware.After appends after the
	//     existing entries — so the counter ends up inside the
	//     retry-replay path and increments once per attempt.
	glueClient := glue.NewFromConfig(cfg, func(o *glue.Options) {
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = 3
		})
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(counter.middleware(), middleware.After)
		})
	})

	// Wire the live *glue.Client (which satisfies GlueClient) into
	// a fresh encoder via the test seam. The encoder will call
	// GetSchemaByDefinition, which the alwaysThrottleTransport
	// will throttle every time.
	enc, err := NewGsrEncoderForTest(glueClient, GsrEncoderOptions{
		RegistryName:                  "test-registry",
		SchemaAutoRegistrationEnabled: false,
	})
	require.NoError(t, err, "test seam encoder construction must not fail")

	schema := &Schema{
		SchemaDefinition: `{"type":"string"}`,
		DataFormat:       "JSON",
		SchemaName:       "phase412-throttle-retry",
	}

	_, encErr := enc.Encode([]byte("payload"), "topic", schema)
	require.Error(t, encErr, "throttled encode must surface an error")

	// The original smithy error must remain in the chain. The Glue
	// SDK surfaces ThrottlingException as *smithy.GenericAPIError;
	// we assert against the smithy.APIError INTERFACE so the test
	// is concrete-type independent (matches spec §3.7 step 5 and
	// the existing newThrottlingError helper).
	var apiErr smithy.APIError
	require.True(t, errors.As(encErr, &apiErr), "smithy.APIError must be in the error chain (got %T: %v)", encErr, encErr)
	require.Equal(t, "ThrottlingException", apiErr.ErrorCode(), "API error code must be ThrottlingException")

	// And the retryer must have run the full MaxAttempts budget.
	// One increment per attempt because the counter is inside the
	// retry loop; if it were at Initialize / Build / Send this
	// assertion would fail at 1.
	require.Equal(t, int32(3), counter.n.Load(),
		"attemptCounter must equal MaxAttempts=3 (Finalize/After-Retry placement runs once per attempt)")
}
