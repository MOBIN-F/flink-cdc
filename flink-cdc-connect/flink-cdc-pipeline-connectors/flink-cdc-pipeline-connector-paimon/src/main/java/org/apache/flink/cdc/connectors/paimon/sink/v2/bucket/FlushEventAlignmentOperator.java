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

import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Align {@link FlushEvent}s broadcasted by {@link BucketAssignOperator}. */
public class FlushEventAlignmentOperator extends AbstractStreamOperatorAdapter<Event>
        implements OneInputStreamOperator<Event, Event> {

    private transient int totalTasksNumber;

    /**
     * Key: subtask id of {@link SchemaOperator}, Value: subtask ids of {@link
     * BucketAssignOperator}.
     */
    private transient Map<Integer, Set<Integer>> sourceTaskIdToAssignBucketSubTaskIds;

    /**
     * Last {@link FlushEvent} forwarded for each source subtask. Copies produced by {@link
     * FlushReplicateOperator} (or by {@code RegularPrePartitionOperator} plus replication) must not
     * start another alignment round. A later schema change on the same table starts a new round.
     */
    private transient Map<Integer, FlushFingerprint> emittedFlushes;

    private transient int currentSubTaskId;

    public FlushEventAlignmentOperator() {
        // It's necessary to avoid unpredictable outcomes of Event shuffling.
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.totalTasksNumber = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.currentSubTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        sourceTaskIdToAssignBucketSubTaskIds = new HashMap<>();
        emittedFlushes = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) {
        Event event = streamRecord.getValue();
        if (event instanceof BucketWrapperFlushEvent) {
            BucketWrapperFlushEvent bucketWrapperFlushEvent = (BucketWrapperFlushEvent) event;
            int sourceSubTaskId = bucketWrapperFlushEvent.getSourceSubTaskId();
            FlushFingerprint fingerprint = FlushFingerprint.from(bucketWrapperFlushEvent);
            if (fingerprint.equals(emittedFlushes.get(sourceSubTaskId))) {
                return;
            }
            Set<Integer> subTaskIds =
                    sourceTaskIdToAssignBucketSubTaskIds.getOrDefault(
                            sourceSubTaskId, new HashSet<>());
            int subtaskId = bucketWrapperFlushEvent.getBucketAssignTaskId();
            subTaskIds.add(subtaskId);
            if (subTaskIds.size() == totalTasksNumber) {
                LOG.info("{} send FlushEvent of {}", currentSubTaskId, sourceSubTaskId);
                output.collect(
                        new StreamRecord<>(
                                new FlushEvent(
                                        sourceSubTaskId,
                                        bucketWrapperFlushEvent.getTableIds(),
                                        bucketWrapperFlushEvent.getSchemaChangeEventType())));
                sourceTaskIdToAssignBucketSubTaskIds.remove(sourceSubTaskId);
                emittedFlushes.put(sourceSubTaskId, fingerprint);
            } else {
                LOG.info(
                        "{} collect FlushEvent of {} with subtask {}",
                        currentSubTaskId,
                        sourceSubTaskId,
                        +subtaskId);
                sourceTaskIdToAssignBucketSubTaskIds.put(sourceSubTaskId, subTaskIds);
            }
        } else {
            TableId schemaChangeTableId = extractSchemaChangeTableId(event);
            if (schemaChangeTableId != null) {
                emittedFlushes
                        .entrySet()
                        .removeIf(entry -> entry.getValue().tableIds.contains(schemaChangeTableId));
            }
            output.collect(streamRecord);
        }
    }

    private static TableId extractSchemaChangeTableId(Event event) {
        if (event instanceof SchemaChangeEvent) {
            return ((SchemaChangeEvent) event).tableId();
        }
        if (event instanceof BucketWrapperChangeEvent) {
            ChangeEvent innerEvent = ((BucketWrapperChangeEvent) event).getInnerEvent();
            if (innerEvent instanceof SchemaChangeEvent) {
                return innerEvent.tableId();
            }
        }
        return null;
    }

    private static final class FlushFingerprint {
        private final SchemaChangeEventType type;
        private final List<TableId> tableIds;

        private FlushFingerprint(SchemaChangeEventType type, List<TableId> tableIds) {
            this.type = type;
            this.tableIds = tableIds;
        }

        private static FlushFingerprint from(BucketWrapperFlushEvent event) {
            return new FlushFingerprint(
                    event.getSchemaChangeEventType(), new ArrayList<>(event.getTableIds()));
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof FlushFingerprint)) {
                return false;
            }
            FlushFingerprint that = (FlushFingerprint) object;
            return type == that.type && Objects.equals(tableIds, that.tableIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, tableIds);
        }
    }
}
