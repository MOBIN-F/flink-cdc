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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SchemaCoordinator}. */
class SchemaCoordinatorTest {

    private static final TableId TABLE_ID = TableId.parse("foo.bar");

    @Test
    void testDefersRequestArrivingDuringSchemaEvolution() throws Exception {
        ExecutorService coordinatorExecutor = Executors.newSingleThreadExecutor();
        MockedOperatorCoordinatorContext context =
                new MockedOperatorCoordinatorContext(
                        new OperatorID(), Thread.currentThread().getContextClassLoader());
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofMillis(300));
        SchemaCoordinator coordinator =
                new SchemaCoordinator(
                        "SchemaCoordinator",
                        context,
                        coordinatorExecutor,
                        metadataApplier,
                        Collections.emptyList(),
                        RouteMode.ALL_MATCH,
                        SchemaChangeBehavior.LENIENT,
                        Duration.ofSeconds(5));

        Schema initialSchema =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, initialSchema);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("name", DataTypes.STRING()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));

        try {
            coordinator.start();

            CompletableFuture<?> createFuture =
                    coordinator.handleCoordinationRequest(
                            new SchemaChangeRequest(0, 0, createTableEvent));
            coordinator.handleEventFromOperator(0, 0, new FlushSuccessEvent(0, 0));
            CompletableFuture<?> addColumnFuture =
                    coordinator.handleCoordinationRequest(
                            new SchemaChangeRequest(0, 0, addColumnEvent));

            createFuture.get(5, TimeUnit.SECONDS);
            coordinator.handleEventFromOperator(0, 0, new FlushSuccessEvent(0, 0));
            addColumnFuture.get(5, TimeUnit.SECONDS);

            List<SchemaChangeEvent> appliedEvents = metadataApplier.getSchemaChangeEvents();
            assertThat(appliedEvents).containsExactly(createTableEvent, addColumnEvent);
            assertThat(context.isJobFailed()).isFalse();
        } finally {
            coordinator.close();
            coordinatorExecutor.shutdownNow();
        }
    }
}
