//go:build integration

package fakeglue

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// TestFakeSatisfiesInterface is the compile-time guarantee that the
// fake stays in lock-step with gsrcore.GlueClient. If the interface
// grows a method, this test won't compile until Fake grows it too.
func TestFakeSatisfiesInterface(t *testing.T) {
	var _ gsrcore.GlueClient = (*Fake)(nil)
	t.Log("fakeglue.Fake satisfies gsrcore.GlueClient")
}

// TestCreateSchemaThenLookup verifies the happy-path that the §5.3
// lifecycle scenarios rely on: CreateSchema stores the schema by
// (registry, name, definition) AND by version-id, and both
// GetSchemaByDefinition and GetSchemaVersion can read it back.
func TestCreateSchemaThenLookup(t *testing.T) {
	f := New()
	ctx := context.Background()

	createOut, err := f.CreateSchema(ctx, &glue.CreateSchemaInput{
		RegistryId:       &types.RegistryId{RegistryName: aws.String("default-registry")},
		SchemaName:       aws.String("test-schema"),
		SchemaDefinition: aws.String("definition"),
		DataFormat:       types.DataFormatAvro,
	})
	require.NoError(t, err)
	require.NotEmpty(t, *createOut.SchemaVersionId)

	defOut, err := f.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String("default-registry"),
			SchemaName:   aws.String("test-schema"),
		},
		SchemaDefinition: aws.String("definition"),
	})
	require.NoError(t, err)
	require.Equal(t, *createOut.SchemaVersionId, *defOut.SchemaVersionId)

	verOut, err := f.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: createOut.SchemaVersionId,
	})
	require.NoError(t, err)
	require.Equal(t, "definition", *verOut.SchemaDefinition)
	require.Equal(t, types.DataFormatAvro, verOut.DataFormat)

	require.Equal(t, 1, f.CallCounts["CreateSchema"])
	require.Equal(t, 1, f.CallCounts["GetSchemaByDefinition"])
	require.Equal(t, 1, f.CallCounts["GetSchemaVersion"])
}

// TestGetSchemaByDefinition_NotFound mirrors the EntityNotFound path
// that triggers auto-register in the production encoder.
func TestGetSchemaByDefinition_NotFound(t *testing.T) {
	f := New()
	_, err := f.GetSchemaByDefinition(context.Background(), &glue.GetSchemaByDefinitionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String("default-registry"),
			SchemaName:   aws.String("missing"),
		},
		SchemaDefinition: aws.String("nope"),
	})
	require.Error(t, err)
	var enf *types.EntityNotFoundException
	require.ErrorAs(t, err, &enf)
}
