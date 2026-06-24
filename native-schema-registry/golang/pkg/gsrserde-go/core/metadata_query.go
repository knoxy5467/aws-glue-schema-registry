package gsrserde

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// QuerySchemaVersionMetadata returns the metadata map associated with a
// schema-version UUID. Flattens the Glue MetadataInfoMap into a plain
// map[string]string by reading each entry's MetadataValue.
//
// Mirrors Java AWSSchemaRegistryClient.java:476-487
// (querySchemaVersionMetadata).
func (s *GsrEncoder) QuerySchemaVersionMetadata(
	ctx context.Context,
	schemaVersionId string,
) (map[string]string, error) {
	resp, err := s.client.QuerySchemaVersionMetadata(ctx, &glue.QuerySchemaVersionMetadataInput{
		SchemaVersionId: &schemaVersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: query schema version metadata: %v", ErrGSR, err)
	}

	result := make(map[string]string, len(resp.MetadataInfoMap))
	for k, info := range resp.MetadataInfoMap {
		if info.MetadataValue != nil {
			result[k] = *info.MetadataValue
		}
	}
	return result, nil
}

// QuerySchemaTags resolves schemaName + schemaDefinition to the schema ARN via
// GetSchemaByDefinition, then fetches the schema's tags via GetTags. Does NOT
// touch the encoder's schema-definition cache — this is a read-side surface.
//
// Mirrors Java AWSSchemaRegistryClient.java:502-517 (querySchemaTags).
func (s *GsrEncoder) QuerySchemaTags(
	ctx context.Context,
	schemaDefinition, schemaName string,
) (map[string]string, error) {
	getResp, err := s.client.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &s.registryName,
			SchemaName:   &schemaName,
		},
		SchemaDefinition: &schemaDefinition,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: get schema by definition: %v", ErrGSR, err)
	}

	tagsResp, err := s.client.GetTags(ctx, &glue.GetTagsInput{
		ResourceArn: getResp.SchemaArn,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: get tags: %v", ErrGSR, err)
	}
	return tagsResp.Tags, nil
}
