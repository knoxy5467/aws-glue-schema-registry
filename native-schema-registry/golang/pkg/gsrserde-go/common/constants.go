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

// Configuration key constants
const (
	// DataFormatTypeKey is the configuration key for data format type.
	DataFormatTypeKey = "DataFormatType"
	// AvroRecordTypeKey is the configuration key for Avro record type.
	AvroRecordTypeKey = "avroRecordType"
	// ProtobufMessageDescriptorKey is the configuration key for Protobuf message descriptor.
	ProtobufMessageDescriptorKey = "protobufMessageDescriptor"
	// JSONObjectTypeKey is the configuration key for JSON object type.
	JSONObjectTypeKey = "jsonObjectType"
	// GSRConfigPathKey is the configuration key for GSR configuration path.
	GSRConfigPathKey = "gsrConfigPath"

	// AvroSpecificTypeKey is the configuration key for a caller-provided
	// reflect.Type used by the Avro deserializer when AvroRecordType ==
	// AvroRecordTypeSpecific. The value must be a reflect.Type for the target
	// Go struct. When absent (nil), SPECIFIC_RECORD deserialization returns
	// ErrMissingAvroSpecificType at the first Deserialize call.
	AvroSpecificTypeKey = "avroSpecificType"

	// ProtobufMessageTypeKey is the configuration key for selecting the
	// protobuf deserialization dispatch mode (POJO vs DYNAMIC_MESSAGE).
	// String value is validated at LoadConfigFromMap time; the resolved
	// ProtobufMessageType enum is carried on common.Configuration.
	ProtobufMessageTypeKey = "protobufMessageType"

	// ProtobufPOJOTypeKey is the configuration key for a caller-provided
	// proto.Message instance used by the protobuf deserializer when
	// ProtobufMessageType == ProtobufMessageTypePOJO. The value must be a
	// proto.Message. When absent (nil), POJO deserialization returns
	// ErrMissingProtobufPOJOType at the first Deserialize call.
	ProtobufPOJOTypeKey = "protobufPOJOType"
)

// DataFormat represents the data format for serialization/deserialization.
type DataFormat int

const (
	// DataFormatUnknown represents an unknown data format.
	DataFormatUnknown DataFormat = iota
	// DataFormatAvro represents Avro data format.
	DataFormatAvro
	// DataFormatJSON represents JSON data format.
	DataFormatJSON
	// DataFormatProtobuf represents Protobuf data format.
	DataFormatProtobuf
)

// String returns the string representation of the DataFormat.
func (d DataFormat) String() string {
	switch d {
	case DataFormatAvro:
		return "AVRO"
	case DataFormatJSON:
		return "JSON"
	case DataFormatProtobuf:
		return "PROTOBUF"
	default:
		return "UNKNOWN"
	}
}
