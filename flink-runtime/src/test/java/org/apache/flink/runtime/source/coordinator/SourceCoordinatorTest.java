/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.DynamicFilteringInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorCheckpointSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.ComponentClosingUtils;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SourceCoordinator}. */
@SuppressWarnings("serial")
class SourceCoordinatorTest extends SourceCoordinatorTestBase {

    @Test
    void testThrowExceptionWhenNotStarted() {
        // The following methods should only be invoked after the source coordinator has started.
        String failureMessage = "Call should fail when source coordinator has not started yet.";
        verifyException(
                () -> sourceCoordinator.notifyCheckpointComplete(100L),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.handleEventFromOperator(0, 0, null),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.executionAttemptFailed(0, 0, null),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>()),
                failureMessage,
                "The coordinator has not started yet.");
    }

    @Test
    void testRestCheckpointAfterCoordinatorStarted() throws Exception {
        // The following methods should only be invoked after the source coordinator has started.
        sourceCoordinator.start();
        verifyException(
                () -> sourceCoordinator.resetToCheckpoint(0L, null),
                "Reset to checkpoint should fail after the coordinator has started",
                "The coordinator can only be reset if it was not yet started");
    }

    @Test
    void testStart() throws Exception {
        sourceCoordinator.start();
        waitForCoordinatorToProcessActions();

        assertThat(getEnumerator().isStarted()).isTrue();
    }

    @Test
    void testClosed() throws Exception {
        sourceCoordinator.start();
        sourceCoordinator.close();
        assertThat(getEnumerator().isClosed()).isTrue();
        assertThat(sourceCoordinator.getContext().isClosed()).isTrue();
    }

    @Test
    void testClosedWithoutStart() throws Exception {
        sourceCoordinator.close();
        assertThat(sourceCoordinator.getContext().isClosed()).isTrue();
    }

    @Test
    void testHandleSourceEvent() throws Exception {
        sourceReady();

        SourceEvent sourceEvent = new SourceEvent() {};
        sourceCoordinator.handleEventFromOperator(0, 0, new SourceEventWrapper(sourceEvent));
        waitForCoordinatorToProcessActions();

        assertThat(getEnumerator().getHandledSourceEvent()).hasSize(1);
        assertThat(getEnumerator().getHandledSourceEvent().get(0)).isEqualTo(sourceEvent);
    }

    @Test
    void testCheckpointCoordinatorAndRestore() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        registerReader(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(100L, checkpointFuture);
        final byte[] bytes = checkpointFuture.get();

        // restore from the checkpoints.
        SourceCoordinator<?, ?> restoredCoordinator = getNewSourceCoordinator();
        restoredCoordinator.resetToCheckpoint(100L, bytes);
        TestingSplitEnumerator<?> restoredEnumerator =
                (TestingSplitEnumerator<?>) restoredCoordinator.getEnumerator();
        SourceCoordinatorContext<?> restoredContext = restoredCoordinator.getContext();
        assertThat(restoredEnumerator.getUnassignedSplits())
                .as("2 splits should have been assigned to reader 0")
                .hasSize(4);
        assertThat(restoredEnumerator.getContext().registeredReaders()).isEmpty();
        assertThat(restoredContext.registeredReaders())
                .as("Registered readers should not be recovered by restoring")
                .isEmpty();
    }

    @Test
    void testBatchSnapshotCoordinatorAndRestore() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        registerReader(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(
                OperatorCoordinator.BATCH_CHECKPOINT_ID, checkpointFuture);
        final byte[] bytes = checkpointFuture.get();

        // restore from the batch snapshot.
        SourceCoordinator<?, ?> restoredCoordinator = getNewSourceCoordinator();
        restoredCoordinator.resetToCheckpoint(OperatorCoordinator.BATCH_CHECKPOINT_ID, bytes);
        TestingSplitEnumerator<?> restoredEnumerator =
                (TestingSplitEnumerator<?>) restoredCoordinator.getEnumerator();
        SourceCoordinatorContext<?> restoredContext = restoredCoordinator.getContext();
        assertThat(restoredEnumerator.getUnassignedSplits())
                .as("2 splits should have been assigned to reader 0")
                .hasSize(4);
        assertThat(restoredEnumerator.getContext().registeredReaders()).isEmpty();
        assertThat(restoredContext.registeredReaders())
                .as("Registered readers should not be recovered by restoring")
                .isEmpty();

        assertThat(restoredContext.getAssignmentTracker().uncheckpointedAssignments())
                .isEqualTo(
                        sourceCoordinator
                                .getContext()
                                .getAssignmentTracker()
                                .uncheckpointedAssignments());
    }

    @Test
    void testSubtaskFailedAndRevertUncompletedAssignments() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        // two splits pending for checkpoint 100
        registerReader(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);
        sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>());

        getEnumerator().addNewSplits(new MockSourceSplit(6));
        getEnumerator().executeAssignOneSplit(0);
        sourceCoordinator.checkpointCoordinator(101L, new CompletableFuture<>());

        // check the state.
        waitForCoordinatorToProcessActions();

        assertThat(getEnumerator().getUnassignedSplits()).hasSize(4);
        assertThat(splitSplitAssignmentTracker.uncheckpointedAssignments()).isEmpty();
        verifyAssignment(
                Arrays.asList("0", "1"),
                splitSplitAssignmentTracker.assignmentsByCheckpointId().get(100L).get(0));
        verifyAssignment(
                Collections.singletonList("2"),
                splitSplitAssignmentTracker.assignmentsByCheckpointId(101L).get(0));

        // none of the checkpoints is confirmed, we fail and revert to the previous one
        sourceCoordinator.executionAttemptFailed(0, 0, null);
        sourceCoordinator.subtaskReset(0, 99L);
        waitForCoordinatorToProcessActions();

        assertThat(context.registeredReaders())
                .as("Reader 0 should have been unregistered.")
                .doesNotContainKey(0);
        // The tracker should have reverted all the splits assignment to reader 0.
        for (Map<Integer, ?> assignment :
                splitSplitAssignmentTracker.assignmentsByCheckpointId().values()) {
            assertThat(assignment)
                    .as("Assignment in uncompleted checkpoint should have been reverted.")
                    .doesNotContainKey(0);
        }
        assertThat(splitSplitAssignmentTracker.uncheckpointedAssignments()).doesNotContainKey(0);
        // The split enumerator should now contains the splits used to b
        // assigned to reader 0.
        assertThat(getEnumerator().getUnassignedSplits()).hasSize(7);
    }

    @Test
    void testFailedSubtaskDoNotRevertCompletedCheckpoint() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        // Assign some splits to reader 0 then take snapshot 100.
        registerReader(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);

        sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>());
        sourceCoordinator.notifyCheckpointComplete(100L);

        sourceCoordinator.executionAttemptFailed(0, 0, null);

        waitForCoordinatorToProcessActions();

        assertThat(getEnumerator().getSuccessfulCheckpoints().get(0)).isEqualTo(100);
        assertThat(context.registeredReaders()).doesNotContainKey(0);
        assertThat(getEnumerator().getUnassignedSplits()).hasSize(4);
        assertThat(splitSplitAssignmentTracker.uncheckpointedAssignments()).doesNotContainKey(0);
        assertThat(splitSplitAssignmentTracker.assignmentsByCheckpointId()).isEmpty();
    }

    @Test
    void testFailJobWhenExceptionThrownFromStart() throws Exception {
        final RuntimeException failureReason = new RuntimeException("Artificial Exception");
        try (final MockSplitEnumeratorContext<MockSourceSplit> enumeratorContext =
                        new MockSplitEnumeratorContext<>(1);
                final SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> splitEnumerator =
                        new MockSplitEnumerator(1, enumeratorContext) {
                            @Override
                            public void start() {
                                throw failureReason;
                            }
                        };
                final SourceCoordinator<?, ?> coordinator =
                        new SourceCoordinator<>(
                                new JobID(),
                                OPERATOR_NAME,
                                new EnumeratorCreatingSource<>(() -> splitEnumerator),
                                context,
                                new CoordinatorStoreImpl(),
                                WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                                null)) {

            coordinator.start();
            waitUtil(
                    () -> operatorCoordinatorContext.isJobFailed(),
                    Duration.ofSeconds(10),
                    "The job should have failed due to the artificial exception.");
            assertThat(operatorCoordinatorContext.getJobFailureReason()).isEqualTo(failureReason);
        }
    }

    @Test
    void testFailJobWhenExceptionThrownFromEnumeratorCreation() throws Exception {
        final RuntimeException failureReason = new RuntimeException("Artificial Exception");

        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        new JobID(),
                        OPERATOR_NAME,
                        new EnumeratorCreatingSource<>(
                                () -> {
                                    throw failureReason;
                                }),
                        context,
                        new CoordinatorStoreImpl(),
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        null);

        coordinator.start();

        assertThat(operatorCoordinatorContext.isJobFailed()).isTrue();
        assertThat(operatorCoordinatorContext.getJobFailureReason()).isEqualTo(failureReason);
    }

    @Test
    void testErrorThrownFromSplitEnumerator() throws Exception {
        final Error error = new Error("Test Error");
        try (final MockSplitEnumeratorContext<MockSourceSplit> enumeratorContext =
                        new MockSplitEnumeratorContext<>(1);
                final SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> splitEnumerator =
                        new MockSplitEnumerator(1, enumeratorContext) {
                            @Override
                            public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
                                throw error;
                            }
                        };
                final SourceCoordinator<?, ?> coordinator =
                        new SourceCoordinator<>(
                                new JobID(),
                                OPERATOR_NAME,
                                new EnumeratorCreatingSource<>(() -> splitEnumerator),
                                context,
                                new CoordinatorStoreImpl(),
                                WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                                null)) {

            coordinator.start();
            coordinator.handleEventFromOperator(1, 0, new SourceEventWrapper(new SourceEvent() {}));

            waitUtil(
                    () -> operatorCoordinatorContext.isJobFailed(),
                    Duration.ofSeconds(10),
                    "The job should have failed due to the artificial exception.");
            assertThat(operatorCoordinatorContext.getJobFailureReason()).isEqualTo(error);
        }
    }

    @Test
    void testBlockOnClose() throws Exception {
        // It is possible that the split enumerator submits some heavy-duty work to the
        // coordinator executor which blocks the coordinator closure.
        final CountDownLatch latch = new CountDownLatch(1);
        try (final MockSplitEnumeratorContext<MockSourceSplit> enumeratorContext =
                        new MockSplitEnumeratorContext<>(1);
                final MockSplitEnumerator splitEnumerator =
                        new MockSplitEnumerator(1, enumeratorContext) {
                            @Override
                            public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
                                context.callAsync(
                                        () -> 1L,
                                        (ignored, t) -> {
                                            latch.countDown();
                                            // Submit a callable that will never return.
                                            try {
                                                Thread.sleep(Long.MAX_VALUE);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            }
                                        });
                            }
                        };
                final SourceCoordinator<?, ?> coordinator =
                        new SourceCoordinator<>(
                                OPERATOR_NAME,
                                new EnumeratorCreatingSource<>(() -> splitEnumerator),
                                context,
                                new CoordinatorStoreImpl())) {

            coordinator.start();
            coordinator.handleEventFromOperator(1, 0, new SourceEventWrapper(new SourceEvent() {}));
            // Wait until the coordinator executor blocks.
            latch.await();

            CompletableFuture<?> future =
                    ComponentClosingUtils.closeAsyncWithTimeout(
                            "testBlockOnClose",
                            (ThrowingRunnable<Exception>) coordinator::close,
                            Duration.ofMillis(1));

            future.exceptionally(
                            e -> {
                                assertThat(e).isInstanceOf(TimeoutException.class);
                                return null;
                            })
                    .get();

            waitUtil(
                    splitEnumerator::closed,
                    Duration.ofSeconds(5),
                    "Split enumerator was not closed in 5 seconds.");
        }
    }

    @Test
    void testUserClassLoaderWhenCreatingNewEnumerator() throws Exception {
        final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
        final OperatorCoordinator.Context context =
                new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);

        final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
                new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);
        final SourceCoordinatorProvider<?> provider =
                new SourceCoordinatorProvider<>(
                        "testOperator",
                        context.getOperatorId(),
                        source,
                        1,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        null);

        final OperatorCoordinator coordinator = provider.getCoordinator(context);
        coordinator.start();

        final ClassLoaderTestEnumerator enumerator = source.createEnumeratorFuture.get();
        assertThat(enumerator.constructorClassLoader).isSameAs(testClassLoader);
        assertThat(enumerator.threadClassLoader.get()).isSameAs(testClassLoader);

        // cleanup
        coordinator.close();
    }

    @Test
    void testUserClassLoaderWhenRestoringEnumerator() throws Exception {
        final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
        final OperatorCoordinator.Context context =
                new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);

        final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
                new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);
        final SourceCoordinatorProvider<?> provider =
                new SourceCoordinatorProvider<>(
                        "testOperator",
                        context.getOperatorId(),
                        source,
                        1,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        null);

        final OperatorCoordinator coordinator = provider.getCoordinator(context);
        coordinator.resetToCheckpoint(1L, createEmptyCheckpoint());
        coordinator.start();

        final ClassLoaderTestEnumerator enumerator = source.restoreEnumeratorFuture.get();
        assertThat(enumerator.constructorClassLoader).isSameAs(testClassLoader);
        assertThat(enumerator.threadClassLoader.get()).isSameAs(testClassLoader);

        // cleanup
        coordinator.close();
    }

    @Test
    void testSerdeBackwardCompatibility() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        // Build checkpoint data with serde version 0
        final TestingSplitEnumerator<MockSourceSplit> enumerator = getEnumerator();
        final Set<MockSourceSplit> splits = new HashSet<>();
        enumerator.runInEnumThreadAndSync(() -> splits.addAll(enumerator.snapshotState(1L)));

        final byte[] checkpointDataForV0Serde = createCheckpointDataWithSerdeV0(splits);

        // Restore from checkpoint data with serde version 0 to test backward compatibility
        SourceCoordinator<?, ?> restoredCoordinator = getNewSourceCoordinator();
        restoredCoordinator.resetToCheckpoint(15213L, checkpointDataForV0Serde);
        TestingSplitEnumerator<?> restoredEnumerator =
                (TestingSplitEnumerator<?>) restoredCoordinator.getEnumerator();
        SourceCoordinatorContext<?> restoredContext = restoredCoordinator.getContext();

        // Check if enumerator is restored correctly
        assertThat(restoredEnumerator.getUnassignedSplits()).isEqualTo(splits);
        assertThat(restoredEnumerator.getHandledSourceEvent()).isEmpty();
        assertThat(restoredContext.registeredReaders()).isEmpty();
    }

    @Test
    public void testSubtaskRestartAndRequestSplitsAgain() throws Exception {
        sourceCoordinator.start();

        final List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            splits.add(new MockSourceSplit(i));
        }
        getEnumerator().addNewSplits(splits);

        int attemptNumber = 0;
        setReaderTaskReady(sourceCoordinator, 0, attemptNumber);
        registerReader(0, attemptNumber);
        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(1);

        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(2);

        sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>());

        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(3);

        assertThat(getEnumerator().getUnassignedSplits()).isEmpty();

        // none of the checkpoints is confirmed, we fail and revert to the previous one
        sourceCoordinator.executionAttemptFailed(0, attemptNumber, null);
        sourceCoordinator.subtaskReset(0, 99L);

        waitUtilNumberReached(() -> getEnumerator().getUnassignedSplits().size(), 2);

        attemptNumber++;
        setReaderTaskReady(sourceCoordinator, 0, attemptNumber);
        registerReader(0, attemptNumber);

        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(4);

        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(5);

        sourceCoordinator.handleEventFromOperator(0, attemptNumber, new RequestSplitEvent());
        waitForSentEvents(6);

        assertThat(getEnumerator().getUnassignedSplits()).isEmpty();

        final List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(0);
        assertAddSplitEvent(events.get(0), Collections.singletonList(splits.get(0)));
        assertAddSplitEvent(events.get(1), Collections.singletonList(splits.get(1)));
        assertAddSplitEvent(events.get(3), Collections.singletonList(splits.get(0)));
        assertAddSplitEvent(events.get(4), Collections.singletonList(splits.get(1)));

        assertThat(events.get(2)).isInstanceOf(NoMoreSplitsEvent.class);
        assertThat(events.get(5)).isInstanceOf(NoMoreSplitsEvent.class);
    }

    @Test
    public void testListeningEventsFromOtherCoordinators() throws Exception {
        final String listeningID = "testListeningID";

        CoordinatorStore store = new CoordinatorStoreImpl();
        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        new JobID(),
                        OPERATOR_NAME,
                        createMockSource(),
                        context,
                        store,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        listeningID);
        coordinator.start();

        assertThat(store.get(listeningID)).isNotNull().isSameAs(coordinator);
    }

    @Test
    public void testInferSourceParallelismAsync() throws Exception {
        final String listeningID = "testListeningID";

        class TestDynamicFilteringEvent implements SourceEvent, DynamicFilteringInfo {}

        CoordinatorStore store = new CoordinatorStoreImpl();
        store.putIfAbsent(listeningID, new SourceEventWrapper(new TestDynamicFilteringEvent()));
        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        new JobID(),
                        OPERATOR_NAME,
                        createMockSource(),
                        context,
                        store,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        listeningID);
        assertThat(coordinator.inferSourceParallelismAsync(2, 1).get()).isEqualTo(2);
    }

    // ------------------------------------------------------------------------
    //  test helpers
    // ------------------------------------------------------------------------

    private byte[] createCheckpointDataWithSerdeV0(Set<MockSourceSplit> splits) throws Exception {

        final MockSplitEnumeratorCheckpointSerializer enumChkptSerializer =
                new MockSplitEnumeratorCheckpointSerializer();
        final DataOutputSerializer serializer = new DataOutputSerializer(32);

        serializer.writeInt(SourceCoordinatorSerdeUtils.VERSION_0);
        serializer.writeInt(enumChkptSerializer.getVersion());

        final byte[] serializedEnumChkpt = enumChkptSerializer.serialize(splits);
        serializer.writeInt(serializedEnumChkpt.length);
        serializer.write(serializedEnumChkpt);

        // Version 0 wrote number of reader, see FLINK-21452
        serializer.writeInt(0);

        // Version 0 wrote split assignment tracker
        serializer.writeInt(0); // SplitSerializer version used in assignment tracker
        serializer.writeInt(0); // Number of checkpoint in assignment tracker

        return serializer.getCopyOfBuffer();
    }

    private static byte[] createEmptyCheckpoint() throws Exception {
        return SourceCoordinator.writeCheckpointBytes(
                Collections.emptySet(), new MockSplitEnumeratorCheckpointSerializer());
    }

    // ------------------------------------------------------------------------
    //  test mocks
    // ------------------------------------------------------------------------

    private static final class ClassLoaderTestEnumerator
            implements SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> {

        final CompletableFuture<ClassLoader> threadClassLoader = new CompletableFuture<>();
        final ClassLoader constructorClassLoader;

        public ClassLoaderTestEnumerator() {
            this.constructorClassLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public void start() {
            threadClassLoader.complete(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addReader(int subtaskId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<MockSourceSplit> snapshotState(long checkpointId) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static final class EnumeratorCreatingSource<
                    T, EnumT extends SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>>>
            implements Source<T, MockSourceSplit, Set<MockSourceSplit>> {

        final CompletableFuture<EnumT> createEnumeratorFuture = new CompletableFuture<>();
        final CompletableFuture<EnumT> restoreEnumeratorFuture = new CompletableFuture<>();
        private final Supplier<EnumT> enumeratorFactory;

        public EnumeratorCreatingSource(Supplier<EnumT> enumeratorFactory) {
            this.enumeratorFactory = enumeratorFactory;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<T, MockSourceSplit> createReader(SourceReaderContext readerContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            final EnumT enumerator = enumeratorFactory.get();
            createEnumeratorFuture.complete(enumerator);
            return enumerator;
        }

        @Override
        public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> restoreEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext,
                Set<MockSourceSplit> checkpoint) {
            final EnumT enumerator = enumeratorFactory.get();
            restoreEnumeratorFuture.complete(enumerator);
            return enumerator;
        }

        @Override
        public SimpleVersionedSerializer<MockSourceSplit> getSplitSerializer() {
            return new MockSourceSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Set<MockSourceSplit>> getEnumeratorCheckpointSerializer() {
            return new MockSplitEnumeratorCheckpointSerializer();
        }
    }
}
