package gsrserde

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// GlueClient is the narrow seam between core/ and the AWS Glue v2 SDK.
//
// The methods on this interface are exactly the Glue calls the Java SerDe
// makes from common/src/main/java/com/amazonaws/services/schemaregistry/common/AWSSchemaRegistryClient.java:
//
//   - GetSchemaByDefinition       (AWSSchemaRegistryClient.java:151, 505)
//   - GetSchemaVersion            (AWSSchemaRegistryClient.java:173, 383)
//   - CreateSchema                (AWSSchemaRegistryClient.java:251) — tags + description ride inline
//   - RegisterSchemaVersion       (AWSSchemaRegistryClient.java:300)
//   - PutSchemaVersionMetadata    (AWSSchemaRegistryClient.java:443) — used by the metadata-write path
//   - QuerySchemaVersionMetadata  (AWSSchemaRegistryClient.java:479) — used by the metadata-read path
//   - GetTags                     (AWSSchemaRegistryClient.java:511) — used by querySchemaTags
//
// CreateRegistry and TagResource are NOT included: Java never calls
// glue.createRegistry or glue.tagResource. Registries are assumed to exist
// (test fixtures, infra-as-code); tags flow into CreateSchema via the .tags()
// builder field at line 341. The original plan listed CreateRegistry +
// TagResource; that was a docs-vs-Java drift that this interface, and the
// matching edit to GSR-Golang-Plan-revision.md §7 Phase 1 item 2, corrects.
type GlueClient interface {
	GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error)
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
	CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error)
	RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error)
	PutSchemaVersionMetadata(ctx context.Context, params *glue.PutSchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error)
	QuerySchemaVersionMetadata(ctx context.Context, params *glue.QuerySchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error)
	GetTags(ctx context.Context, params *glue.GetTagsInput, optFns ...func(*glue.Options)) (*glue.GetTagsOutput, error)
}
