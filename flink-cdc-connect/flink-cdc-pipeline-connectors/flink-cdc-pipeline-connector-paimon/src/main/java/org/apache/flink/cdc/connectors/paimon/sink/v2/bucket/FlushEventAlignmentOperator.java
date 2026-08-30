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
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Align {@link FlushEvent}s broadcasted by {@link BucketAssignOperator}.
 *
 * <p>Schema operator subtasks participate in every evolution round, but they may currently process
 * {@link FlushEvent}s from different source partitions (for example two Kafka tables whose first
 * {@code CREATE TABLE} events arrive on different partitions). Alignment therefore waits until
 * every bucket-assigner has reported in the current round, instead of requiring all assigners to
 * flush the same source partition.
 */
public class FlushEventAlignmentOperator extends AbstractStreamOperatorAdapter<Event>
        implements OneInputStreamOperator<Event, Event> {

    private transient int totalTasksNumber;

    /**
     * Bucket-assigner subtask ids that have reported a {@link FlushEvent} in the current evolution
     * round. {@link SchemaOperator} subtasks always join the same round, even when they flush
     * different source partitions.
     */
    private transient Set<Integer> flushedAssigners;

    private transient Set<TableId> pendingTableIds;

    private transient int pendingSourceSubTaskId;

    private transient SchemaChangeEventType pendingType;

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
        resetBarrier();
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) {
        Event event = streamRecord.getValue();
        if (event instanceof BucketWrapperFlushEvent) {
            BucketWrapperFlushEvent bucketWrapperFlushEvent = (BucketWrapperFlushEvent) event;
            flushedAssigners.add(bucketWrapperFlushEvent.getBucketAssignTaskId());
            pendingTableIds.addAll(bucketWrapperFlushEvent.getTableIds());
            if (pendingSourceSubTaskId < 0) {
                pendingSourceSubTaskId = bucketWrapperFlushEvent.getSourceSubTaskId();
            }
            SchemaChangeEventType eventType = bucketWrapperFlushEvent.getSchemaChangeEventType();
            if (pendingType == null || eventType == SchemaChangeEventType.CREATE_TABLE) {
                pendingType = eventType;
            }
            if (flushedAssigners.size() == totalTasksNumber) {
                LOG.info(
                        "{} send FlushEvent of source {} after assigners {} reported",
                        currentSubTaskId,
                        pendingSourceSubTaskId,
                        flushedAssigners);
                output.collect(
                        new StreamRecord<>(
                                new FlushEvent(
                                        pendingSourceSubTaskId,
                                        new ArrayList<>(pendingTableIds),
                                        pendingType)));
                resetBarrier();
            } else {
                LOG.info(
                        "{} collect FlushEvent of {} with assigner {}",
                        currentSubTaskId,
                        bucketWrapperFlushEvent.getSourceSubTaskId(),
                        bucketWrapperFlushEvent.getBucketAssignTaskId());
            }
        } else {
            output.collect(streamRecord);
        }
    }

    private void resetBarrier() {
        flushedAssigners = new HashSet<>();
        pendingTableIds = new LinkedHashSet<>();
        pendingSourceSubTaskId = -1;
        pendingType = null;
    }
}
