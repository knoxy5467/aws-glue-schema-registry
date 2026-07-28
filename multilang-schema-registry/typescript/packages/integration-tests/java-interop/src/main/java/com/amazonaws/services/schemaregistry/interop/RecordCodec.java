/*
 * Per-format helpers for converting between the JSON envelope on the wire
 * and the typed Java records that GlueSchemaRegistryKafkaSerializer /
 * Deserializer expect on the Java side.
 *
 * Envelope shapes (per format):
 *   AVRO:     { "fields": { "<name>": <value>, ... } }
 *   JSON:     { "schema": "<jsonSchema>", "payload": "<jsonDoc>" }
 *   PROTOBUF: { "messageTypeFullName": "<test.TestMessage>", "fieldsJson": "<json>" }
 *
 * The shape mirrors the per-format Java record types
 * (GenericRecord / JsonDataWithSchema / DynamicMessage) — same fields, same
 * ordering, lossless round-trip.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Optional;

public final class RecordCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RecordCodec() {}

    /**
     * Builds a typed Java record from the per-format JSON envelope in the
     * request body. Throws IllegalArgumentException for unknown formats.
     */
    public static Object buildRecord(String format, String schemaDef, JsonNode envelope) throws Exception {
        switch (format) {
            case "AVRO":
                return buildAvroRecord(schemaDef, envelope);
            case "JSON":
                return buildJsonRecord(schemaDef, envelope);
            case "PROTOBUF":
                return buildProtoRecord(schemaDef, envelope);
            default:
                throw new IllegalArgumentException("unsupported format: " + format);
        }
    }

    /**
     * Inverse of buildRecord: converts a typed Java record returned by
     * GlueSchemaRegistryKafkaDeserializer back into the JSON envelope.
     */
    public static ObjectNode recordToEnvelope(String format, Object record) throws Exception {
        switch (format) {
            case "AVRO":
                return avroToEnvelope((GenericRecord) record);
            case "JSON":
                return jsonToEnvelope((JsonDataWithSchema) record);
            case "PROTOBUF":
                return protoToEnvelope((Message) record);
            default:
                throw new IllegalArgumentException("unsupported format: " + format);
        }
    }

    private static GenericRecord buildAvroRecord(String schemaDef, JsonNode envelope) throws java.io.IOException {
        Schema schema = new Schema.Parser().parse(schemaDef);
        JsonNode fields = envelope.get("fields");
        if (fields == null || !fields.isObject()) {
            throw new IllegalArgumentException("AVRO envelope missing object 'fields'");
        }
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            JsonNode val = fields.get(f.name());
            if (val == null || val.isNull()) {
                continue;
            }
            record.put(f.name(), avroCoerce(f.schema(), val));
        }
        return record;
    }

    private static Object avroCoerce(Schema fieldSchema, JsonNode val) throws java.io.IOException {
        Schema effective = fieldSchema;
        if (effective.getType() == Schema.Type.UNION) {
            // Pick the first non-null branch — sufficient for the matrix of
            // simple test schemas used by the interop suite.
            for (Schema branch : effective.getTypes()) {
                if (branch.getType() != Schema.Type.NULL) {
                    effective = branch;
                    break;
                }
            }
        }
        switch (effective.getType()) {
            case STRING:
                return val.asText();
            case INT:
                return val.asInt();
            case LONG:
                return val.asLong();
            case FLOAT:
                return (float) val.asDouble();
            case DOUBLE:
                return val.asDouble();
            case BOOLEAN:
                return val.asBoolean();
            case BYTES:
                return ByteBuffer.wrap(val.binaryValue());
            case FIXED: {
                byte[] raw = val.binaryValue();
                if (raw.length != effective.getFixedSize()) {
                    throw new IllegalArgumentException("avro FIXED " + effective.getFullName()
                            + ": expected " + effective.getFixedSize() + " bytes, got " + raw.length);
                }
                return new GenericData.Fixed(effective, raw);
            }
            case ENUM: {
                String sym = val.asText();
                if (!effective.getEnumSymbols().contains(sym)) {
                    throw new IllegalArgumentException("avro ENUM " + effective.getFullName()
                            + ": symbol " + sym + " not in " + effective.getEnumSymbols());
                }
                return new GenericData.EnumSymbol(effective, sym);
            }
            case ARRAY: {
                if (!val.isArray()) {
                    throw new IllegalArgumentException("avro ARRAY field expects JSON array, got " + val.getNodeType());
                }
                java.util.List<Object> out = new java.util.ArrayList<>(val.size());
                Schema elem = effective.getElementType();
                for (JsonNode child : val) {
                    out.add(avroCoerce(elem, child));
                }
                return new GenericData.Array<>(effective, out);
            }
            case MAP: {
                if (!val.isObject()) {
                    throw new IllegalArgumentException("avro MAP field expects JSON object, got " + val.getNodeType());
                }
                java.util.LinkedHashMap<String, Object> out = new java.util.LinkedHashMap<>();
                Schema valueSchema = effective.getValueType();
                java.util.Iterator<java.util.Map.Entry<String, JsonNode>> it = val.fields();
                while (it.hasNext()) {
                    java.util.Map.Entry<String, JsonNode> e = it.next();
                    out.put(e.getKey(), avroCoerce(valueSchema, e.getValue()));
                }
                return out;
            }
            case RECORD: {
                if (!val.isObject()) {
                    throw new IllegalArgumentException("avro RECORD field expects JSON object, got " + val.getNodeType());
                }
                GenericData.Record nested = new GenericData.Record(effective);
                for (Schema.Field f : effective.getFields()) {
                    JsonNode child = val.get(f.name());
                    if (child == null || child.isNull()) {
                        continue;
                    }
                    nested.put(f.name(), avroCoerce(f.schema(), child));
                }
                return nested;
            }
            default:
                throw new IllegalArgumentException("avro coerce: unsupported field type " + effective.getType());
        }
    }

    private static JsonDataWithSchema buildJsonRecord(String schemaDef, JsonNode envelope) {
        JsonNode payload = envelope.get("payload");
        if (payload == null) {
            throw new IllegalArgumentException("JSON envelope missing 'payload'");
        }
        String payloadStr = payload.isTextual() ? payload.asText() : payload.toString();
        return JsonDataWithSchema.builder(schemaDef, payloadStr).build();
    }

    private static DynamicMessage buildProtoRecord(String schemaDef, JsonNode envelope) throws Exception {
        JsonNode msgFullName = envelope.get("messageTypeFullName");
        JsonNode fieldsJson = envelope.get("fieldsJson");
        if (msgFullName == null || fieldsJson == null) {
            throw new IllegalArgumentException("PROTOBUF envelope missing messageTypeFullName or fieldsJson");
        }
        // FileDescriptorUtils.protoFileToFileDescriptor takes a BASENAME
        // and the package name SEPARATELY — ProtobufSchemaLoader builds
        // the package's directory hierarchy in its FakeFileSystem and
        // resolves the basename within it. Passing a path-shaped filename
        // (e.g. "test/default.proto") makes wire-schema complain about
        // parent directories. Always pass plain "default.proto".
        String pkg = extractProtoPackage(schemaDef);
        Descriptors.FileDescriptor fileDesc = FileDescriptorUtils.protoFileToFileDescriptor(
                schemaDef, "default.proto",
                (pkg != null && !pkg.isEmpty()) ? Optional.of(pkg) : Optional.empty());
        Descriptors.Descriptor msgDesc = findMessageDescriptor(fileDesc, msgFullName.asText());
        if (msgDesc == null) {
            throw new IllegalArgumentException("protobuf message type not found in schema: " + msgFullName.asText());
        }
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(msgDesc);
        JsonFormat.parser().ignoringUnknownFields().merge(fieldsJson.asText(), builder);
        return builder.build();
    }

    /**
     * Pulls the `package` declaration out of a .proto source string. Returns
     * empty string when the file has no package line. Tolerates trailing
     * inline comments and arbitrary whitespace between `package` and the
     * name. Block comments and string literals are out of scope — .proto
     * files don't carry those at the file level.
     */
    private static String extractProtoPackage(String schemaDef) {
        for (String rawLine : schemaDef.split("\n")) {
            String line = rawLine;
            int comment = line.indexOf("//");
            if (comment >= 0) {
                line = line.substring(0, comment);
            }
            String trimmed = line.trim();
            if (!trimmed.startsWith("package")) {
                continue;
            }
            // Require whitespace or non-identifier char after `package`.
            if (trimmed.length() <= "package".length()
                    || Character.isJavaIdentifierPart(trimmed.charAt("package".length()))) {
                continue;
            }
            String rest = trimmed.substring("package".length()).trim();
            if (rest.endsWith(";")) {
                rest = rest.substring(0, rest.length() - 1).trim();
            }
            if (!rest.isEmpty()) {
                return rest;
            }
        }
        return "";
    }

    /**
     * Resolves a full message name (possibly ".package.Name") against a
     * FileDescriptor by scanning top-level + nested message types. Returns
     * null when not found.
     *
     * Only exact full-name matches are accepted. A short-name fallback was
     * intentionally removed: it silently picked the wrong descriptor when
     * the schema had multiple messages sharing a simple name (e.g. nested
     * types or two top-level messages in different packages).
     */
    private static Descriptors.Descriptor findMessageDescriptor(Descriptors.FileDescriptor fileDesc, String fullName) {
        // Strip leading dot if present.
        String want = fullName.startsWith(".") ? fullName.substring(1) : fullName;
        for (Descriptors.Descriptor d : fileDesc.getMessageTypes()) {
            Descriptors.Descriptor found = matchOrSearch(d, want);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptors.Descriptor matchOrSearch(Descriptors.Descriptor d, String want) {
        if (d.getFullName().equals(want)) {
            return d;
        }
        for (Descriptors.Descriptor nested : d.getNestedTypes()) {
            Descriptors.Descriptor found = matchOrSearch(nested, want);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static ObjectNode avroToEnvelope(GenericRecord record) {
        ObjectNode env = MAPPER.createObjectNode();
        ObjectNode fields = MAPPER.createObjectNode();
        for (Schema.Field f : record.getSchema().getFields()) {
            Object v = record.get(f.name());
            avroPutInto(fields, f.name(), v);
        }
        env.set("fields", fields);
        return env;
    }

    private static void avroPutInto(ObjectNode fields, String name, Object v) {
        fields.set(name, avroValueToJson(v));
    }

    /**
     * Converts an Avro generic-data value (returned by GenericRecord.get) into
     * a Jackson JsonNode the test side can compare against. Recurses into
     * ARRAY / MAP / RECORD / FIXED / ENUM so future matrix expansions can use
     * nested types without re-tripping on this codec.
     */
    private static com.fasterxml.jackson.databind.JsonNode avroValueToJson(Object v) {
        if (v == null) {
            return MAPPER.nullNode();
        }
        if (v instanceof CharSequence) {
            return MAPPER.getNodeFactory().textNode(v.toString());
        }
        if (v instanceof Integer) {
            return MAPPER.getNodeFactory().numberNode((Integer) v);
        }
        if (v instanceof Long) {
            return MAPPER.getNodeFactory().numberNode((Long) v);
        }
        if (v instanceof Float) {
            return MAPPER.getNodeFactory().numberNode((Float) v);
        }
        if (v instanceof Double) {
            return MAPPER.getNodeFactory().numberNode((Double) v);
        }
        if (v instanceof Boolean) {
            return MAPPER.getNodeFactory().booleanNode((Boolean) v);
        }
        if (v instanceof byte[]) {
            return MAPPER.getNodeFactory().binaryNode((byte[]) v);
        }
        if (v instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) v;
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return MAPPER.getNodeFactory().binaryNode(bytes);
        }
        if (v instanceof GenericData.Fixed) {
            return MAPPER.getNodeFactory().binaryNode(((GenericData.Fixed) v).bytes());
        }
        if (v instanceof GenericData.EnumSymbol) {
            return MAPPER.getNodeFactory().textNode(v.toString());
        }
        if (v instanceof java.util.List<?>) {
            com.fasterxml.jackson.databind.node.ArrayNode arr = MAPPER.createArrayNode();
            for (Object item : (java.util.List<?>) v) {
                arr.add(avroValueToJson(item));
            }
            return arr;
        }
        if (v instanceof java.util.Map<?, ?>) {
            ObjectNode obj = MAPPER.createObjectNode();
            for (java.util.Map.Entry<?, ?> e : ((java.util.Map<?, ?>) v).entrySet()) {
                obj.set(e.getKey().toString(), avroValueToJson(e.getValue()));
            }
            return obj;
        }
        if (v instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) v;
            ObjectNode obj = MAPPER.createObjectNode();
            for (Schema.Field f : rec.getSchema().getFields()) {
                obj.set(f.name(), avroValueToJson(rec.get(f.name())));
            }
            return obj;
        }
        // Fallback: stringify so the round-trip doesn't drop data.
        return MAPPER.getNodeFactory().textNode(v.toString());
    }

    private static ObjectNode jsonToEnvelope(JsonDataWithSchema record) {
        ObjectNode env = MAPPER.createObjectNode();
        env.put("schema", record.getSchema());
        env.put("payload", record.getPayload());
        return env;
    }

    private static ObjectNode protoToEnvelope(Message record) throws Exception {
        ObjectNode env = MAPPER.createObjectNode();
        env.put("messageTypeFullName", record.getDescriptorForType().getFullName());
        String json = JsonFormat.printer().preservingProtoFieldNames().includingDefaultValueFields().print(record);
        env.put("fieldsJson", json);
        return env;
    }
}
