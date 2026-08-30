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

package org.apache.flink.cdc.connectors.paimon.sink.v2.bucket;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/** Tests for flush replication and alignment used by the Paimon pre-write topology. */
class FlushReplicateAndAlignmentTest {

    private static final TableId CUSTOMERS = TableId.tableId("inventory", "customers");
    private static final TableId ORDERS = TableId.tableId("inventory", "orders");

    @Test
    void testReplicateFlushToEverySubtaskAndKeepDataLocal() throws Exception {
        FlushReplicateOperator operator = new FlushReplicateOperator();
        CollectingOutput<Tuple2<Integer, Event>> output = new CollectingOutput<>();
        setField(operator, "output", output);
        setField(operator, "parallelism", 2);
        setField(operator, "subtaskId", 0);

        operator.processElement(new StreamRecord<>(flushEvent(0, CUSTOMERS)));
        Assertions.assertThat(output.records)
                .extracting(record -> record.getValue().f0)
                .containsExactly(0, 1);

        output.records.clear();
        DataChangeEventStub data = new DataChangeEventStub();
        operator.processElement(new StreamRecord<>(data));
        Assertions.assertThat(output.records).hasSize(1);
        Assertions.assertThat(output.records.get(0).getValue().f0).isEqualTo(0);
        Assertions.assertThat(output.records.get(0).getValue().f1).isSameAs(data);
    }

    @Test
    void testAlignmentWaitsForAllAssignersPerSource() throws Exception {
        FlushEventAlignmentOperator operator = alignmentOperator(2);
        CollectingOutput<Event> output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        Assertions.assertThat(output.records).isEmpty();
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);
        FlushEvent flushEvent = (FlushEvent) output.records.get(0).getValue();
        Assertions.assertThat(flushEvent.getSourceSubTaskId()).isEqualTo(0);
        Assertions.assertThat(flushEvent.getTableIds()).containsExactly(CUSTOMERS);
    }

    @Test
    void testAlignmentIgnoresReplicatedCopiesOfTheSameFlush() throws Exception {
        FlushEventAlignmentOperator operator = alignmentOperator(2);
        CollectingOutput<Event> output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);
    }

    @Test
    void testDataChangeDoesNotResetFlushFingerprint() throws Exception {
        FlushEventAlignmentOperator operator = alignmentOperator(2);
        CollectingOutput<Event> output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);

        operator.processElement(new StreamRecord<>(new DataChangeEventStub()));
        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(2);
        Assertions.assertThat(output.records.get(1).getValue())
                .isInstanceOf(DataChangeEventStub.class);
    }

    @Test
    void testSchemaChangeAllowsAnotherFlushRoundForSameTable() throws Exception {
        FlushEventAlignmentOperator operator = alignmentOperator(2);
        CollectingOutput<Event> output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);

        Schema schema =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();
        operator.processElement(new StreamRecord<>(new CreateTableEvent(CUSTOMERS, schema)));
        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(
                        output.records.stream()
                                .filter(record -> record.getValue() instanceof FlushEvent)
                                .count())
                .isEqualTo(2);
    }

    @Test
    void testConcurrentCreatesFromDifferentSourcesBothComplete() throws Exception {
        FlushEventAlignmentOperator operator = alignmentOperator(2);
        CollectingOutput<Event> output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        operator.processElement(flushRecord(1, 1, ORDERS));
        Assertions.assertThat(output.records).isEmpty();

        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        operator.processElement(flushRecord(1, 0, ORDERS));
        List<Integer> sources =
                output.records.stream()
                        .map(record -> ((FlushEvent) record.getValue()).getSourceSubTaskId())
                        .collect(Collectors.toList());
        Assertions.assertThat(sources).containsExactlyInAnyOrder(0, 1);
    }

    private static FlushEventAlignmentOperator alignmentOperator(int parallelism) throws Exception {
        FlushEventAlignmentOperator operator = new FlushEventAlignmentOperator();
        setField(operator, "totalTasksNumber", parallelism);
        setField(operator, "currentSubTaskId", 0);
        setField(operator, "sourceTaskIdToAssignBucketSubTaskIds", new HashMap<>());
        setField(operator, "emittedFlushes", new HashMap<>());
        return operator;
    }

    private static CollectingOutput<Event> collectTo(FlushEventAlignmentOperator operator)
            throws Exception {
        CollectingOutput<Event> output = new CollectingOutput<>();
        setField(operator, "output", output);
        return output;
    }

    private static FlushEvent flushEvent(int sourceSubTaskId, TableId tableId) {
        return new FlushEvent(
                sourceSubTaskId,
                Collections.singletonList(tableId),
                SchemaChangeEventType.CREATE_TABLE);
    }

    private static StreamRecord<Event> flushRecord(
            int sourceSubTaskId, int assignerId, TableId tableId) {
        return new StreamRecord<>(
                new BucketWrapperFlushEvent(
                        0,
                        sourceSubTaskId,
                        assignerId,
                        Collections.singletonList(tableId),
                        SchemaChangeEventType.CREATE_TABLE));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> current = target.getClass();
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static final class DataChangeEventStub implements Event {}

    private static class CollectingOutput<T> implements Output<StreamRecord<T>> {
        private final List<StreamRecord<T>> records = new ArrayList<>();

        public void emitWatermark(org.apache.flink.runtime.event.WatermarkEvent watermark) {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> streamRecord) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void collect(StreamRecord<T> record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }
}
