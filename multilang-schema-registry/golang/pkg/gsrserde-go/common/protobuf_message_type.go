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

// ProtobufMessageType controls how the protobuf deserializer allocates the
// target message on deserialization. Mirrors Java's
// com.amazonaws.services.schemaregistry.utils.ProtobufMessageType enum.
type ProtobufMessageType int

const (
	// ProtobufMessageTypeUnknown is the zero value / unset sentinel. The
	// deserializer treats it identically to ProtobufMessageTypeDynamic.
	ProtobufMessageTypeUnknown ProtobufMessageType = iota

	// ProtobufMessageTypePOJO directs the deserializer to unmarshal into the
	// caller-provided proto.Message instance (set via ProtobufPOJOMessage on
	// common.Configuration). Returns a concrete generated-type message.
	// Java parity: ProtobufMessageType.POJO.
	ProtobufMessageTypePOJO

	// ProtobufMessageTypeDynamic directs the deserializer to allocate a
	// dynamicpb.Message using the schema's message descriptor. This is the
	// current default behavior and is schema-driven rather than type-driven.
	// Java parity: ProtobufMessageType.DYNAMIC_MESSAGE.
	ProtobufMessageTypeDynamic
)

// String returns the Java-canonical string representation of the
// ProtobufMessageType, matching Java's ProtobufMessageType.name() output.
func (p ProtobufMessageType) String() string {
	switch p {
	case ProtobufMessageTypePOJO:
		return "POJO"
	case ProtobufMessageTypeDynamic:
		return "DYNAMIC_MESSAGE"
	default:
		return "Unknown"
	}
}
