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
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SchemaCoordinator}. */
class SchemaCoordinatorTest {

    private static final TableId TABLE_ID = TableId.parse("foo.bar");
    private static final int PARALLELISM = 2;

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

    @Test
    void testDefersBroadcastRequestsWhenParallelismGreaterThanOne() throws Exception {
        ExecutorService coordinatorExecutor = Executors.newSingleThreadExecutor();
        MockedOperatorCoordinatorContext context = mockedContext(PARALLELISM);
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofMillis(300));
        SchemaCoordinator coordinator = coordinator(context, coordinatorExecutor, metadataApplier);

        CreateTableEvent createTableEvent = createTableEvent();
        AddColumnEvent addColumnEvent = addColumnEvent("name");

        try {
            coordinator.start();

            List<CompletableFuture<?>> createFutures =
                    requestBroadcast(coordinator, 0, createTableEvent, PARALLELISM);
            flushAll(coordinator, PARALLELISM);
            waitUntilApplied(metadataApplier, 1);

            List<CompletableFuture<?>> addColumnFutures =
                    requestBroadcast(coordinator, 0, addColumnEvent, PARALLELISM);

            awaitAll(createFutures);
            flushAll(coordinator, PARALLELISM);
            awaitAll(addColumnFutures);

            assertThat(metadataApplier.getSchemaChangeEvents())
                    .containsExactly(createTableEvent, addColumnEvent);
            assertThat(context.isJobFailed()).isFalse();
        } finally {
            coordinator.close();
            coordinatorExecutor.shutdownNow();
        }
    }

    @Test
    void testProcessesMultipleDeferredRoundsWhenParallelismGreaterThanOne() throws Exception {
        ExecutorService coordinatorExecutor = Executors.newSingleThreadExecutor();
        MockedOperatorCoordinatorContext context = mockedContext(PARALLELISM);
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofMillis(300));
        SchemaCoordinator coordinator = coordinator(context, coordinatorExecutor, metadataApplier);

        CreateTableEvent createTableEvent = createTableEvent();
        AddColumnEvent addNameEvent = addColumnEvent("name");
        AddColumnEvent addEmailEvent = addColumnEvent("email");

        try {
            coordinator.start();

            List<CompletableFuture<?>> createFutures =
                    requestBroadcast(coordinator, 0, createTableEvent, PARALLELISM);
            flushAll(coordinator, PARALLELISM);
            waitUntilApplied(metadataApplier, 1);

            List<CompletableFuture<?>> addNameFutures =
                    requestBroadcast(coordinator, 0, addNameEvent, PARALLELISM);

            awaitAll(createFutures);
            flushAll(coordinator, PARALLELISM);
            waitUntilApplied(metadataApplier, 2);

            List<CompletableFuture<?>> addEmailFutures =
                    requestBroadcast(coordinator, 0, addEmailEvent, PARALLELISM);

            awaitAll(addNameFutures);
            flushAll(coordinator, PARALLELISM);
            awaitAll(addEmailFutures);

            assertThat(metadataApplier.getSchemaChangeEvents())
                    .containsExactly(createTableEvent, addNameEvent, addEmailEvent);
            assertThat(context.isJobFailed()).isFalse();
        } finally {
            coordinator.close();
            coordinatorExecutor.shutdownNow();
        }
    }

    @Test
    void testFailsDeferredRequestsWhenParallelEvolutionFails() throws Exception {
        ExecutorService coordinatorExecutor = Executors.newSingleThreadExecutor();
        MockedOperatorCoordinatorContext context = mockedContext(PARALLELISM);
        Set<SchemaChangeEventType> enabledEventTypes =
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(
                        Duration.ofMillis(300),
                        enabledEventTypes,
                        Collections.singleton(SchemaChangeEventType.CREATE_TABLE));
        SchemaCoordinator coordinator = coordinator(context, coordinatorExecutor, metadataApplier);

        CreateTableEvent createTableEvent = createTableEvent();
        AddColumnEvent addColumnEvent = addColumnEvent("name");

        try {
            coordinator.start();

            List<CompletableFuture<?>> createFutures =
                    requestBroadcast(coordinator, 0, createTableEvent, PARALLELISM);
            flushAll(coordinator, PARALLELISM);
            waitUntilApplied(metadataApplier, 1);

            List<CompletableFuture<?>> addColumnFutures =
                    requestBroadcast(coordinator, 0, addColumnEvent, PARALLELISM);
            // Let the coordinator event loop enqueue the broadcast requests before apply fails.
            Thread.sleep(100L);

            for (CompletableFuture<?> future : createFutures) {
                assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
            }
            for (CompletableFuture<?> future : addColumnFutures) {
                assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
            }
            assertThat(context.isJobFailed()).isTrue();
            assertThat(metadataApplier.getSchemaChangeEvents()).containsExactly(createTableEvent);
        } finally {
            coordinator.close();
            coordinatorExecutor.shutdownNow();
        }
    }

    private static MockedOperatorCoordinatorContext mockedContext(int parallelism) {
        return new MockedOperatorCoordinatorContext(
                new OperatorID(), parallelism, Thread.currentThread().getContextClassLoader());
    }

    private static SchemaCoordinator coordinator(
            MockedOperatorCoordinatorContext context,
            ExecutorService coordinatorExecutor,
            CollectingMetadataApplier metadataApplier) {
        return new SchemaCoordinator(
                "SchemaCoordinator",
                context,
                coordinatorExecutor,
                metadataApplier,
                Collections.emptyList(),
                RouteMode.ALL_MATCH,
                SchemaChangeBehavior.LENIENT,
                Duration.ofSeconds(5));
    }

    private static CreateTableEvent createTableEvent() {
        return new CreateTableEvent(
                TABLE_ID,
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build());
    }

    private static AddColumnEvent addColumnEvent(String columnName) {
        return new AddColumnEvent(
                TABLE_ID,
                Collections.singletonList(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn(columnName, DataTypes.STRING()),
                                AddColumnEvent.ColumnPosition.LAST,
                                null)));
    }

    private static List<CompletableFuture<?>> requestBroadcast(
            SchemaCoordinator coordinator,
            int sourceSubTaskId,
            SchemaChangeEvent event,
            int parallelism) {
        List<CompletableFuture<?>> futures = new ArrayList<>(parallelism);
        for (int sinkSubTaskId = 0; sinkSubTaskId < parallelism; sinkSubTaskId++) {
            futures.add(
                    coordinator.handleCoordinationRequest(
                            new SchemaChangeRequest(sourceSubTaskId, sinkSubTaskId, event)));
        }
        return futures;
    }

    private static void flushAll(SchemaCoordinator coordinator, int parallelism) {
        for (int sinkSubTaskId = 0; sinkSubTaskId < parallelism; sinkSubTaskId++) {
            coordinator.handleEventFromOperator(
                    sinkSubTaskId, 0, new FlushSuccessEvent(sinkSubTaskId, 0));
        }
    }

    private static void awaitAll(List<CompletableFuture<?>> futures) throws Exception {
        for (CompletableFuture<?> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
    }

    private static void waitUntilApplied(CollectingMetadataApplier metadataApplier, int count)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            if (metadataApplier.getSchemaChangeEvents().size() >= count) {
                return;
            }
            Thread.sleep(10L);
        }
        assertThat(metadataApplier.getSchemaChangeEvents())
                .as("Timed out waiting for %s applied schema change events", count)
                .hasSizeGreaterThanOrEqualTo(count);
    }
}
