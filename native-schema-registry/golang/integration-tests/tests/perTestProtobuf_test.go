package integration_tests

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PerfTestSuite struct {
	suite.Suite
}

func (p *PerfTestSuite) TestSerializeProtobufTenThousand() {
	message := &testpb.TestMessage{
		Id:    "sarama-test-789",
		Name:  "Sarama Integration Test",
		Age:   28,
		Email: "sarama@example.com",
		Tags:  []string{"integration", "test", "sarama", "protobuf", "gsr", "pure-go"},
	}

	// Extract the message descriptor using protobuf reflection
	messageDescriptor := message.ProtoReflect().Descriptor()

	gsrConfigAbsolutePath, err := filepath.Abs("./gsr.properties")
	if err != nil {
		p.T().Fatal("Failed to get absolute path of gsr.properties")
	}
	// Create Protobuf configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
		common.GSRConfigPathKey:             gsrConfigAbsolutePath,
	}
	config := common.NewConfiguration(configMap)

	// Step 1: Create Serializer with GSR configuration
	gsr_serializer, err := serializer.NewSerializer(config)
	if err != nil {
		p.T().Log("could not create gsr serializer")
	}

	// Step 2: Serialize the message (auto-registers schema with GSR)
	p.T().Logf("Serializing %T message", message)
	gsrEncodedData, err := gsr_serializer.Serialize("test-topic", message)
	require.NoError(p.T(), err, "Serializer.Serialize should succeed")
	require.NotEmpty(p.T(), gsrEncodedData, "Serialized data should not be empty")
	p.T().Logf("Serialized message: %d bytes", len(gsrEncodedData))

	p.T().Logf("starting Fifty Thousand")
	startTime := time.Now()
	for i := 0; i < 50000; i++ {
		_, _ = gsr_serializer.Serialize("test-topic", message)
	}
	endTime := time.Now()

	totalTime := endTime.Second() - startTime.Second()
	p.T().Logf("total time for 10000 messages: %d", totalTime)

}

func TestPerfIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PerfTestSuite))
}
