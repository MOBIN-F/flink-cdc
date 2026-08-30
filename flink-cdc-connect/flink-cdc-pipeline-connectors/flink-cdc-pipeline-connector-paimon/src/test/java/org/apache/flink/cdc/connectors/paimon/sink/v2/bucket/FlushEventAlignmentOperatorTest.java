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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Tests for {@link FlushEventAlignmentOperator}.
 *
 * <p>Two tables whose first {@code CREATE TABLE} events arrive on different source partitions used
 * to deadlock: alignment waited for every bucket-assigner to flush the same source id.
 */
class FlushEventAlignmentOperatorTest {

    private static final TableId CUSTOMERS = TableId.tableId("inventory", "customers");
    private static final TableId ORDERS = TableId.tableId("inventory", "orders");

    @Test
    void testSameSourceFlushFromAllAssigners() throws Exception {
        FlushEventAlignmentOperator operator = operator(2);
        CollectingOutput output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        Assertions.assertThat(output.records).isEmpty();

        operator.processElement(flushRecord(0, 1, CUSTOMERS));
        Assertions.assertThat(output.records).hasSize(1);
        FlushEvent flushEvent = (FlushEvent) output.records.get(0).getValue();
        Assertions.assertThat(flushEvent.getSourceSubTaskId()).isEqualTo(0);
        Assertions.assertThat(flushEvent.getTableIds()).containsExactly(CUSTOMERS);
        Assertions.assertThat(flushEvent.getSchemaChangeEventType())
                .isEqualTo(SchemaChangeEventType.CREATE_TABLE);
    }

    @Test
    void testDifferentSourceFlushFromDifferentAssignersDoesNotDeadlock() throws Exception {
        FlushEventAlignmentOperator operator = operator(2);
        CollectingOutput output = collectTo(operator);

        operator.processElement(flushRecord(0, 0, CUSTOMERS));
        Assertions.assertThat(output.records).isEmpty();

        operator.processElement(flushRecord(1, 1, ORDERS));
        Assertions.assertThat(output.records).hasSize(1);
        FlushEvent flushEvent = (FlushEvent) output.records.get(0).getValue();
        Assertions.assertThat(flushEvent.getTableIds()).containsExactly(CUSTOMERS, ORDERS);
        Assertions.assertThat(flushEvent.getSchemaChangeEventType())
                .isEqualTo(SchemaChangeEventType.CREATE_TABLE);
    }

    private static FlushEventAlignmentOperator operator(int parallelism) throws Exception {
        FlushEventAlignmentOperator operator = new FlushEventAlignmentOperator();
        setField(operator, "totalTasksNumber", parallelism);
        setField(operator, "currentSubTaskId", 0);
        setField(operator, "flushedAssigners", new HashSet<>());
        setField(operator, "pendingTableIds", new LinkedHashSet<>());
        setField(operator, "pendingSourceSubTaskId", -1);
        setField(operator, "pendingType", null);
        return operator;
    }

    private static CollectingOutput collectTo(FlushEventAlignmentOperator operator)
            throws Exception {
        CollectingOutput output = new CollectingOutput();
        setField(operator, "output", output);
        return output;
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

    private static class CollectingOutput implements Output<StreamRecord<Event>> {
        private final List<StreamRecord<Event>> records = new ArrayList<>();

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
        public void collect(StreamRecord<Event> record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }
}
