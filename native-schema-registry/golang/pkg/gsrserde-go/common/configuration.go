// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Configuration holds the Glue Schema Registry configuration.
type Configuration struct {
	DataFormat                DataFormat
	AvroRecordType            AvroRecordType
	ProtobufMessageDescriptor protoreflect.MessageDescriptor
	JsonObjectType            reflect.Type
	GsrConfig                 map[string]string
	AdditionalProperties      map[string]interface{}

	// AvroSpecificType is the Go struct type used for SPECIFIC_RECORD
	// deserialization. Non-nil only when AvroRecordType == AvroRecordTypeSpecific.
	// Set via the AvroSpecificTypeKey config entry. When nil and
	// AvroRecordType == AvroRecordTypeSpecific, the Avro deserializer returns
	// ErrMissingAvroSpecificType at Deserialize time (not at config parse time,
	// because this is a programmatic Go API rather than a string configMap key).
	AvroSpecificType reflect.Type

	// ProtobufMessageType selects the dispatch mode for the protobuf
	// deserializer: POJO (caller-provided concrete type) vs DYNAMIC_MESSAGE
	// (schema-driven dynamicpb). Zero value (ProtobufMessageTypeUnknown) is
	// treated as ProtobufMessageTypeDynamic.
	ProtobufMessageType ProtobufMessageType

	// ProtobufPOJOMessage is the caller-provided proto.Message used as the
	// unmarshal target when ProtobufMessageType == ProtobufMessageTypePOJO.
	// The deserializer clones this value via proto.Clone before each
	// Deserialize call. When nil and ProtobufMessageType == POJO, the
	// deserializer returns ErrMissingProtobufPOJOType.
	//
	// MUST be a zero-value instance — non-zero field values on the template
	// will leak into the deserialized result for any wire fields absent from
	// the payload, because proto.Clone deep-copies the entire template before
	// unmarshaling over it.
	ProtobufPOJOMessage proto.Message
}
var (

	ErrNilConfig = fmt.Errorf("config cannot be nil")
)

// NewConfiguration creates a new Configuration from a map of configuration values.
func NewConfiguration(configs map[string]interface{}) *Configuration {
	c := &Configuration{
		DataFormat:           DataFormatUnknown,
		AvroRecordType:       AvroRecordTypeUnknown,
		AdditionalProperties: make(map[string]interface{}),
	}
	c.buildConfigs(configs)
	return c
}


func (c *Configuration) buildConfigs(configs map[string]interface{}) {
	c.validateAndSetAvroRecordType(configs)
	c.validateAndSetProtobufMessageDescriptor(configs)
	c.validateAndSetDataFormat(configs)
	c.validateAndSetJSONObjectType(configs)
	c.validateAndSetGsrConfig(configs)
	c.validateAndSetAvroSpecificType(configs)
	c.validateAndSetProtobufMessageType(configs)
	c.validateAndSetProtobufPOJOMessage(configs)
}
func (c *Configuration) validateAndSetGsrConfig(configs map[string]interface{}) {
	if val, ok := configs[GSRConfigPathKey]; ok {
		if gsrConfig, ok := val.(map[string]string); ok {
			c.GsrConfig = gsrConfig
		}
	}
}

func (c *Configuration) validateAndSetAvroSpecificType(configs map[string]interface{}) {
	if val, ok := configs[AvroSpecificTypeKey]; ok {
		if specificType, ok := val.(reflect.Type); ok {
			c.AvroSpecificType = specificType
		}
	}
}

func (c *Configuration) validateAndSetProtobufMessageType(configs map[string]interface{}) {
	if val, ok := configs[ProtobufMessageTypeKey]; ok {
		if msgType, ok := val.(ProtobufMessageType); ok {
			c.ProtobufMessageType = msgType
		}
	}
}

func (c *Configuration) validateAndSetProtobufPOJOMessage(configs map[string]interface{}) {
	if val, ok := configs[ProtobufPOJOTypeKey]; ok {
		if msg, ok := val.(proto.Message); ok {
			c.ProtobufPOJOMessage = msg
		}
	}
}

func (c *Configuration) validateAndSetDataFormat(configs map[string]interface{}) {
	if val, ok := configs[DataFormatTypeKey]; ok {
		if dataFormat, ok := val.(DataFormat); ok {
			c.DataFormat = dataFormat
		}
	}
}

func (c *Configuration) validateAndSetAvroRecordType(configs map[string]interface{}) {
	if val, ok := configs[AvroRecordTypeKey]; ok {
		if avroRecordType, ok := val.(AvroRecordType); ok {
			c.AvroRecordType = avroRecordType
		}
	}
}

func (c *Configuration) validateAndSetProtobufMessageDescriptor(configs map[string]interface{}) {
	if val, ok := configs[ProtobufMessageDescriptorKey]; ok {
		if messageDescriptor, ok := val.(protoreflect.MessageDescriptor); ok {
			c.ProtobufMessageDescriptor = messageDescriptor
		}
	}
}

func (c *Configuration) validateAndSetJSONObjectType(configs map[string]interface{}) {
	if val, ok := configs[JSONObjectTypeKey]; ok {
		if jsonType, ok := val.(reflect.Type); ok {
			c.JsonObjectType = jsonType
		}
	}
}
