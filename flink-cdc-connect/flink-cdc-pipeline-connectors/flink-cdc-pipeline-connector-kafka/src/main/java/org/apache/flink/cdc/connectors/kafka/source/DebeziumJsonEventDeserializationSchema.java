/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Converts schema-enabled Debezium JSON Kafka records into pipeline events. */
public class DebeziumJsonEventDeserializationSchema
        implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;
    private transient Map<TableId, TableSchemaState> globalTableSchemas;
    private transient Map<PartitionTableKey, Schema> partitionTableSchemas;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        initialize();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out)
            throws IOException {
        initialize();
        if (record.value() == null) {
            return;
        }
        JsonNode root = mapper.readTree(record.value());
        JsonNode payload = root.path("payload");
        JsonNode source = payload.path("source");
        JsonNode opNode = payload.path("op");
        if (payload.isMissingNode()
                || payload.isNull()
                || opNode.isMissingNode()
                || opNode.isNull()
                || source.path("db").isMissingNode()
                || source.path("table").isMissingNode()) {
            return;
        }

        String operation = opNode.asText();
        if (!operation.equals("r")
                && !operation.equals("c")
                && !operation.equals("u")
                && !operation.equals("d")) {
            return;
        }
        TableId tableId =
                TableId.tableId(source.path("db").asText(), source.path("table").asText());
        JsonNode rowSchemaNode = findFieldSchema(root.path("schema"), "after");
        if (rowSchemaNode == null) {
            rowSchemaNode = findFieldSchema(root.path("schema"), "before");
        }
        if (rowSchemaNode == null || !rowSchemaNode.path("fields").isArray()) {
            throw failure(record, "Debezium value does not contain a before/after row schema.");
        }

        Schema incomingSchema = parseSchema(rowSchemaNode);
        PartitionTableKey partitionTableKey =
                new PartitionTableKey(record.topic(), record.partition(), tableId);
        Schema partitionSchema = partitionTableSchemas.get(partitionTableKey);
        if (partitionSchema != null) {
            validatePartitionEvolution(record, partitionSchema, incomingSchema);
        }

        TableSchemaState state = globalTableSchemas.get(tableId);
        List<Event> schemaEvents = new ArrayList<>();
        if (state == null) {
            state = new TableSchemaState(incomingSchema);
            globalTableSchemas.put(tableId, state);
            schemaEvents.add(new CreateTableEvent(tableId, incomingSchema));
        } else {
            evolveGlobalSchema(record, tableId, state, incomingSchema, schemaEvents);
        }
        partitionTableSchemas.put(partitionTableKey, incomingSchema);
        for (Event schemaEvent : schemaEvents) {
            out.collect(schemaEvent);
        }

        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("topic", record.topic());
        meta.put("partition", String.valueOf(record.partition()));
        meta.put("offset", String.valueOf(record.offset()));
        RecordData before = convertRecord(payload.get("before"), state.schema);
        RecordData after = convertRecord(payload.get("after"), state.schema);
        switch (operation) {
            case "r":
            case "c":
                out.collect(DataChangeEvent.insertEvent(tableId, require(after, "after"), meta));
                break;
            case "u":
                out.collect(
                        DataChangeEvent.updateEvent(
                                tableId, require(before, "before"), require(after, "after"), meta));
                break;
            case "d":
                out.collect(DataChangeEvent.deleteEvent(tableId, require(before, "before"), meta));
                break;
            default:
                throw new IllegalStateException("Unexpected Debezium operation " + operation);
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    private void initialize() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        if (globalTableSchemas == null) {
            globalTableSchemas = new HashMap<>();
        }
        if (partitionTableSchemas == null) {
            partitionTableSchemas = new HashMap<>();
        }
    }

    private Schema parseSchema(JsonNode rowSchema) {
        Schema.Builder builder = Schema.newBuilder();
        for (JsonNode field : rowSchema.path("fields")) {
            builder.physicalColumn(
                    requiredText(field, "field", "Debezium row schema field"), parseType(field));
        }
        return builder.build();
    }

    private DataType parseType(JsonNode schema) {
        String logicalName = schema.path("name").asText("");
        DataType type;
        switch (logicalName) {
            case "io.debezium.time.Date":
                type = DataTypes.DATE();
                break;
            case "io.debezium.time.Time":
                type = DataTypes.TIME(3);
                break;
            case "io.debezium.time.MicroTime":
                type = DataTypes.TIME(6);
                break;
            case "io.debezium.time.NanoTime":
                type = DataTypes.TIME(9);
                break;
            case "io.debezium.time.Timestamp":
                type = DataTypes.TIMESTAMP(3);
                break;
            case "io.debezium.time.MicroTimestamp":
                type = DataTypes.TIMESTAMP(6);
                break;
            case "io.debezium.time.NanoTimestamp":
                type = DataTypes.TIMESTAMP(9);
                break;
            case "io.debezium.time.ZonedTimestamp":
                type = DataTypes.TIMESTAMP_TZ(9);
                break;
            case "io.debezium.time.Year":
                type = DataTypes.INT();
                break;
            case "io.debezium.data.Bits":
                type =
                        DataTypes.VARBINARY(
                                positiveParameter(schema, "length").orElse(Integer.MAX_VALUE));
                break;
            case "io.debezium.data.Enum":
            case "io.debezium.data.Json":
                type = DataTypes.STRING();
                break;
            case "org.apache.kafka.connect.data.Decimal":
                int scale = schema.path("parameters").path("scale").asInt(0);
                int precision =
                        schema.path("parameters").path("connect.decimal.precision").asInt(38);
                type = DataTypes.DECIMAL(Math.min(38, precision), Math.min(scale, precision));
                break;
            default:
                type = parsePrimitiveType(schema);
        }
        return schema.path("optional").asBoolean(true) ? type.nullable() : type.notNull();
    }

    private DataType parsePrimitiveType(JsonNode schema) {
        String type = schema.path("type").asText();
        switch (type) {
            case "int8":
                return DataTypes.TINYINT();
            case "int16":
                return DataTypes.SMALLINT();
            case "int32":
                return DataTypes.INT();
            case "int64":
                return DataTypes.BIGINT();
            case "float":
            case "float32":
                return DataTypes.FLOAT();
            case "double":
            case "float64":
                return DataTypes.DOUBLE();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "bytes":
                return DataTypes.BYTES();
            case "string":
                // Kafka Connect has no VARCHAR; MySQL CHAR/VARCHAR/TEXT all become string.
                return DataTypes.STRING();
            default:
                throw new IllegalArgumentException(
                        "Unsupported Debezium schema type '" + type + "'.");
        }
    }

    private Optional<Integer> positiveParameter(JsonNode schema, String name) {
        JsonNode value = schema.path("parameters").path(name);
        if (value.isMissingNode() || value.isNull()) {
            return Optional.empty();
        }
        try {
            int parsed = Integer.parseInt(value.asText());
            return parsed > 0 ? Optional.of(parsed) : Optional.empty();
        } catch (NumberFormatException ignored) {
            return Optional.empty();
        }
    }

    private void validatePartitionEvolution(
            ConsumerRecord<byte[], byte[]> record, Schema previous, Schema incoming) {
        Map<String, Column> incomingColumns = columnsByName(incoming);
        for (Column previousColumn : previous.getColumns()) {
            Column incomingColumn = incomingColumns.get(previousColumn.getName());
            if (incomingColumn == null) {
                // Dropped or renamed source columns stay in the widest schema. New names are
                // added later; missing values are coerced to null.
                continue;
            }
            DataType merged = mergeType(previousColumn.getType(), incomingColumn.getType());
            if (merged == null || !merged.equals(incomingColumn.getType())) {
                throw failure(
                        record,
                        "Incompatible or narrowing type change for column '"
                                + previousColumn.getName()
                                + "' within Kafka partition: "
                                + previousColumn.getType()
                                + " -> "
                                + incomingColumn.getType()
                                + ".");
            }
        }
    }

    private void evolveGlobalSchema(
            ConsumerRecord<byte[], byte[]> record,
            TableId tableId,
            TableSchemaState state,
            Schema incoming,
            List<Event> events) {
        List<Column> widestColumns = new ArrayList<>(state.schema.getColumns());
        List<AddColumnEvent.ColumnWithPosition> additions = new ArrayList<>();
        Map<String, DataType> alteredTypes = new LinkedHashMap<>();
        Map<String, DataType> oldTypes = new LinkedHashMap<>();
        Map<String, Integer> currentPositions = new HashMap<>();
        for (int i = 0; i < widestColumns.size(); i++) {
            currentPositions.put(widestColumns.get(i).getName(), i);
        }
        Map<String, Column> incomingColumns = columnsByName(incoming);
        for (int i = 0; i < widestColumns.size(); i++) {
            Column currentColumn = widestColumns.get(i);
            if (!incomingColumns.containsKey(currentColumn.getName())
                    && !currentColumn.getType().isNullable()) {
                DataType nullableType = currentColumn.getType().nullable();
                widestColumns.set(i, Column.physicalColumn(currentColumn.getName(), nullableType));
                alteredTypes.put(currentColumn.getName(), nullableType);
                oldTypes.put(currentColumn.getName(), currentColumn.getType());
            }
        }
        for (Column incomingColumn : incoming.getColumns()) {
            Integer position = currentPositions.get(incomingColumn.getName());
            if (position == null) {
                Column nullableColumn =
                        Column.physicalColumn(
                                incomingColumn.getName(), incomingColumn.getType().nullable());
                currentPositions.put(nullableColumn.getName(), widestColumns.size());
                widestColumns.add(nullableColumn);
                additions.add(AddColumnEvent.last(nullableColumn));
                continue;
            }
            Column currentColumn = widestColumns.get(position);
            DataType merged = mergeType(currentColumn.getType(), incomingColumn.getType());
            if (merged == null) {
                DataType reverseMerged =
                        mergeType(incomingColumn.getType(), currentColumn.getType());
                if (reverseMerged != null && reverseMerged.equals(currentColumn.getType())) {
                    continue;
                }
                throw failure(
                        record,
                        "Incompatible type change for column '"
                                + incomingColumn.getName()
                                + "': "
                                + currentColumn.getType()
                                + " versus "
                                + incomingColumn.getType()
                                + ".");
            }
            if (!merged.equals(currentColumn.getType())) {
                widestColumns.set(position, Column.physicalColumn(currentColumn.getName(), merged));
                alteredTypes.put(currentColumn.getName(), merged);
                oldTypes.put(currentColumn.getName(), currentColumn.getType());
            }
        }
        if (!additions.isEmpty()) {
            events.add(new AddColumnEvent(tableId, additions));
        }
        if (!alteredTypes.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, alteredTypes, oldTypes));
        }
        if (!additions.isEmpty() || !alteredTypes.isEmpty()) {
            state.schema = state.schema.copy(widestColumns);
        }
    }

    private DataType mergeType(DataType current, DataType incoming) {
        boolean nullable = current.isNullable() || incoming.isNullable();
        DataType currentNullable = current.copy(nullable);
        DataType incomingNullable = incoming.copy(nullable);
        if (currentNullable.equals(incomingNullable)) {
            return currentNullable;
        }
        // STRING is the universal widening target used by SchemaMergingUtils. Replay of an
        // INT → STRING change (MySQL ALTER to VARCHAR) must follow the same rule.
        if (incoming.is(DataTypeRoot.VARCHAR)) {
            return DataTypes.STRING().copy(nullable);
        }
        int currentRank = integerRank(current.getTypeRoot());
        int incomingRank = integerRank(incoming.getTypeRoot());
        if (currentRank > 0 && incomingRank > currentRank) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.FLOAT) && incoming.is(DataTypeRoot.DOUBLE)) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.VARCHAR)
                && incoming.is(DataTypeRoot.VARCHAR)
                && DataTypes.getLength(incoming).orElse(0)
                        > DataTypes.getLength(current).orElse(0)) {
            return DataTypes.VARCHAR(DataTypes.getLength(incoming).getAsInt()).copy(nullable);
        }
        if (current.is(DataTypeRoot.VARBINARY)
                && incoming.is(DataTypeRoot.VARBINARY)
                && DataTypes.getLength(incoming).orElse(0)
                        > DataTypes.getLength(current).orElse(0)) {
            return DataTypes.VARBINARY(DataTypes.getLength(incoming).getAsInt()).copy(nullable);
        }
        if (current.getTypeRoot() == incoming.getTypeRoot()
                && DataTypes.getPrecision(current).isPresent()
                && DataTypes.getPrecision(incoming).isPresent()
                && DataTypes.getPrecision(incoming).getAsInt()
                        > DataTypes.getPrecision(current).getAsInt()) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.DECIMAL) && incoming.is(DataTypeRoot.DECIMAL)) {
            DecimalType left = (DecimalType) current;
            DecimalType right = (DecimalType) incoming;
            int scale = Math.max(left.getScale(), right.getScale());
            int integerDigits =
                    Math.max(
                            left.getPrecision() - left.getScale(),
                            right.getPrecision() - right.getScale());
            int precision = integerDigits + scale;
            if (precision <= 38 && (precision > left.getPrecision() || scale > left.getScale())) {
                return DataTypes.DECIMAL(precision, scale).copy(nullable);
            }
        }
        return null;
    }

    private int integerRank(DataTypeRoot root) {
        switch (root) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
                return 3;
            case BIGINT:
                return 4;
            default:
                return 0;
        }
    }

    private RecordData convertRecord(JsonNode row, Schema targetSchema) {
        if (row == null || row.isNull()) {
            return null;
        }
        GenericRecordData result = new GenericRecordData(targetSchema.getColumnCount());
        for (int i = 0; i < targetSchema.getColumnCount(); i++) {
            Column column = targetSchema.getColumns().get(i);
            result.setField(i, convertValue(row.get(column.getName()), column.getType()));
        }
        return result;
    }

    private Object convertValue(JsonNode node, DataType targetType) {
        if (node == null || node.isNull()) {
            return null;
        }
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return (byte) node.asInt();
            case SMALLINT:
                return (short) node.asInt();
            case INTEGER:
                return node.asInt();
            case BIGINT:
                return node.asLong();
            case FLOAT:
                return (float) node.asDouble();
            case DOUBLE:
                return node.asDouble();
            case BOOLEAN:
                return node.asBoolean();
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(node.asText());
            case BINARY:
            case VARBINARY:
                return node.isBinary()
                        ? binaryValue(node)
                        : Base64.getDecoder()
                                .decode(node.asText().getBytes(StandardCharsets.UTF_8));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) targetType;
                return DecimalData.fromBigDecimal(
                        decimalValue(node, decimalType.getScale()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case DATE:
                return node.isIntegralNumber()
                        ? DateData.fromEpochDay(node.asInt())
                        : DateData.fromIsoLocalDateString(node.asText());
            case TIME_WITHOUT_TIME_ZONE:
                return node.isIntegralNumber()
                        ? TimeData.fromNanoOfDay(
                                normalizeTimeToNanos(
                                        node.asLong(),
                                        DataTypes.getPrecision(targetType).orElse(3)))
                        : TimeData.fromIsoLocalTimeString(node.asText());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return node.isIntegralNumber()
                        ? TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(
                                                normalizeTimestampToMillis(
                                                        node.asLong(),
                                                        DataTypes.getPrecision(targetType)
                                                                .orElse(3))),
                                        ZoneOffset.UTC))
                        : TimestampData.fromLocalDateTime(LocalDateTime.parse(node.asText()));
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.fromZonedDateTime(ZonedDateTime.parse(node.asText()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported target type " + targetType.asSummaryString() + ".");
        }
    }

    private byte[] binaryValue(JsonNode node) {
        try {
            return node.binaryValue();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot decode Debezium binary value.", e);
        }
    }

    private BigDecimal decimalValue(JsonNode node, int scale) {
        if (node.isNumber()) {
            return node.decimalValue();
        }
        try {
            return new BigDecimal(node.asText());
        } catch (NumberFormatException ignored) {
            byte[] unscaled = Base64.getDecoder().decode(node.asText());
            return new BigDecimal(new BigInteger(unscaled), scale);
        }
    }

    private long normalizeTimeToNanos(long value, int precision) {
        if (precision <= 3) {
            return value * 1_000_000L;
        }
        if (precision <= 6) {
            return value * 1_000L;
        }
        return value;
    }

    private long normalizeTimestampToMillis(long value, int precision) {
        if (precision > 6) {
            return value / 1_000_000L;
        }
        if (precision > 3) {
            return value / 1_000L;
        }
        return value;
    }

    private JsonNode findFieldSchema(JsonNode envelopeSchema, String fieldName) {
        for (JsonNode field : envelopeSchema.path("fields")) {
            if (fieldName.equals(field.path("field").asText())) {
                return field;
            }
        }
        return null;
    }

    private Map<String, Column> columnsByName(Schema schema) {
        Map<String, Column> result = new HashMap<>();
        for (Column column : schema.getColumns()) {
            result.put(column.getName(), column);
        }
        return result;
    }

    private String requiredText(JsonNode node, String field, String description) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isEmpty()) {
            throw new IllegalArgumentException(description + " is missing '" + field + "'.");
        }
        return value.asText();
    }

    private RecordData require(RecordData record, String name) {
        return Objects.requireNonNull(record, "Debezium operation requires non-null " + name + ".");
    }

    private IllegalArgumentException failure(
            ConsumerRecord<byte[], byte[]> record, String message) {
        return new IllegalArgumentException(
                message
                        + " Kafka position "
                        + record.topic()
                        + "-"
                        + record.partition()
                        + "@"
                        + record.offset());
    }

    private static class TableSchemaState {
        private Schema schema;

        private TableSchemaState(Schema schema) {
            this.schema = schema;
        }
    }

    private static class PartitionTableKey {
        private final String topic;
        private final int partition;
        private final TableId tableId;

        private PartitionTableKey(String topic, int partition, TableId tableId) {
            this.topic = topic;
            this.partition = partition;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof PartitionTableKey)) {
                return false;
            }
            PartitionTableKey that = (PartitionTableKey) object;
            return partition == that.partition
                    && Objects.equals(topic, that.topic)
                    && Objects.equals(tableId, that.tableId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, tableId);
        }
    }
}
