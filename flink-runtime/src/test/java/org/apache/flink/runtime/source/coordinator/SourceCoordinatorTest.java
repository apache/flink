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

import org.apache.flink.api.connector.source.Boundedness;
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
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.source.event.SourceEventWrapper;

import org.junit.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Unit tests for {@link SourceCoordinator}. */
@SuppressWarnings("serial")
public class SourceCoordinatorTest extends SourceCoordinatorTestBase {

    @Test
    public void testThrowExceptionWhenNotStarted() {
        // The following methods should only be invoked after the source coordinator has started.
        String failureMessage = "Call should fail when source coordinator has not started yet.";
        verifyException(
                () -> sourceCoordinator.notifyCheckpointComplete(100L),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.handleEventFromOperator(0, null),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.subtaskFailed(0, null),
                failureMessage,
                "The coordinator has not started yet.");
        verifyException(
                () -> sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>()),
                failureMessage,
                "The coordinator has not started yet.");
    }

    @Test
    public void testRestCheckpointAfterCoordinatorStarted() throws Exception {
        // The following methods should only be invoked after the source coordinator has started.
        sourceCoordinator.start();
        verifyException(
                () -> sourceCoordinator.resetToCheckpoint(0L, null),
                "Reset to checkpoint should fail after the coordinator has started",
                "The coordinator can only be reset if it was not yet started");
    }

    @Test
    public void testStart() throws Exception {
        sourceCoordinator.start();
        waitForCoordinatorToProcessActions();

        assertTrue(getEnumerator().isStarted());
    }

    @Test
    public void testClosed() throws Exception {
        sourceCoordinator.start();
        sourceCoordinator.close();
        assertTrue(getEnumerator().isClosed());
    }

    @Test
    public void testHandleSourceEvent() throws Exception {
        sourceReady();

        SourceEvent sourceEvent = new SourceEvent() {};
        sourceCoordinator.handleEventFromOperator(0, new SourceEventWrapper(sourceEvent));
        waitForCoordinatorToProcessActions();

        assertEquals(1, getEnumerator().getHandledSourceEvent().size());
        assertEquals(sourceEvent, getEnumerator().getHandledSourceEvent().get(0));
    }

    @Test
    public void testCheckpointCoordinatorAndRestore() throws Exception {
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
        assertEquals(
                "2 splits should have been assigned to reader 0",
                4,
                restoredEnumerator.getUnassignedSplits().size());
        assertTrue(restoredEnumerator.getContext().registeredReaders().isEmpty());
        assertEquals(
                "Registered readers should not be recovered by restoring",
                0,
                restoredContext.registeredReaders().size());
    }

    @Test
    public void testSubtaskFailedAndRevertUncompletedAssignments() throws Exception {
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

        assertEquals(4, getEnumerator().getUnassignedSplits().size());
        assertTrue(splitSplitAssignmentTracker.uncheckpointedAssignments().isEmpty());
        verifyAssignment(
                Arrays.asList("0", "1"),
                splitSplitAssignmentTracker.assignmentsByCheckpointId().get(100L).get(0));
        verifyAssignment(
                Collections.singletonList("2"),
                splitSplitAssignmentTracker.assignmentsByCheckpointId(101L).get(0));

        // none of the checkpoints is confirmed, we fail and revert to the previous one
        sourceCoordinator.subtaskFailed(0, null);
        sourceCoordinator.subtaskReset(0, 99L);
        waitForCoordinatorToProcessActions();

        assertFalse(
                "Reader 0 should have been unregistered.",
                context.registeredReaders().containsKey(0));
        // The tracker should have reverted all the splits assignment to reader 0.
        for (Map<Integer, ?> assignment :
                splitSplitAssignmentTracker.assignmentsByCheckpointId().values()) {
            assertFalse(
                    "Assignment in uncompleted checkpoint should have been reverted.",
                    assignment.containsKey(0));
        }
        assertFalse(splitSplitAssignmentTracker.uncheckpointedAssignments().containsKey(0));
        // The split enumerator should now contains the splits used to b
        // assigned to reader 0.
        assertEquals(7, getEnumerator().getUnassignedSplits().size());
    }

    @Test
    public void testFailedSubtaskDoNotRevertCompletedCheckpoint() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        // Assign some splits to reader 0 then take snapshot 100.
        registerReader(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);

        sourceCoordinator.checkpointCoordinator(100L, new CompletableFuture<>());
        sourceCoordinator.notifyCheckpointComplete(100L);

        sourceCoordinator.subtaskFailed(0, null);

        waitForCoordinatorToProcessActions();

        assertEquals(100L, (long) getEnumerator().getSuccessfulCheckpoints().get(0));
        assertFalse(context.registeredReaders().containsKey(0));
        assertEquals(4, getEnumerator().getUnassignedSplits().size());
        assertFalse(splitSplitAssignmentTracker.uncheckpointedAssignments().containsKey(0));
        assertTrue(splitSplitAssignmentTracker.assignmentsByCheckpointId().isEmpty());
    }

    @Test
    public void testFailJobWhenExceptionThrownFromStart() throws Exception {
        final RuntimeException failureReason = new RuntimeException("Artificial Exception");

        final SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> splitEnumerator =
                new MockSplitEnumerator(1, new MockSplitEnumeratorContext<>(1)) {
                    @Override
                    public void start() {
                        throw failureReason;
                    }
                };

        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        OPERATOR_NAME,
                        coordinatorExecutor,
                        new EnumeratorCreatingSource<>(() -> splitEnumerator),
                        context);

        coordinator.start();
        waitUtil(
                () -> operatorCoordinatorContext.isJobFailed(),
                Duration.ofSeconds(10),
                "The job should have failed due to the artificial exception.");
        assertEquals(failureReason, operatorCoordinatorContext.getJobFailureReason());
    }

    @Test
    public void testFailJobWhenExceptionThrownFromEnumeratorCreation() throws Exception {
        final RuntimeException failureReason = new RuntimeException("Artificial Exception");

        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        OPERATOR_NAME,
                        coordinatorExecutor,
                        new EnumeratorCreatingSource<>(
                                () -> {
                                    throw failureReason;
                                }),
                        context);

        coordinator.start();

        assertTrue(operatorCoordinatorContext.isJobFailed());
        assertEquals(failureReason, operatorCoordinatorContext.getJobFailureReason());
    }

    @Test
    public void testErrorThrownFromSplitEnumerator() throws Exception {
        final Error error = new Error("Test Error");

        final SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> splitEnumerator =
                new MockSplitEnumerator(1, new MockSplitEnumeratorContext<>(1)) {
                    @Override
                    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
                        throw error;
                    }
                };

        final SourceCoordinator<?, ?> coordinator =
                new SourceCoordinator<>(
                        OPERATOR_NAME,
                        coordinatorExecutor,
                        new EnumeratorCreatingSource<>(() -> splitEnumerator),
                        context);

        coordinator.start();
        coordinator.handleEventFromOperator(1, new SourceEventWrapper(new SourceEvent() {}));

        waitUtil(
                () -> operatorCoordinatorContext.isJobFailed(),
                Duration.ofSeconds(10),
                "The job should have failed due to the artificial exception.");
        assertEquals(error, operatorCoordinatorContext.getJobFailureReason());
    }

    @Test
    public void testUserClassLoaderWhenCreatingNewEnumerator() throws Exception {
        final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
        final OperatorCoordinator.Context context =
                new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);

        final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
                new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);
        final SourceCoordinatorProvider<?> provider =
                new SourceCoordinatorProvider<>("testOperator", context.getOperatorId(), source, 1);

        final OperatorCoordinator coordinator = provider.getCoordinator(context);
        coordinator.start();

        final ClassLoaderTestEnumerator enumerator = source.createEnumeratorFuture.get();
        assertSame(testClassLoader, enumerator.constructorClassLoader);
        assertSame(testClassLoader, enumerator.threadClassLoader.get());

        // cleanup
        coordinator.close();
    }

    @Test
    public void testUserClassLoaderWhenRestoringEnumerator() throws Exception {
        final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
        final OperatorCoordinator.Context context =
                new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);

        final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
                new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);
        final SourceCoordinatorProvider<?> provider =
                new SourceCoordinatorProvider<>("testOperator", context.getOperatorId(), source, 1);

        final OperatorCoordinator coordinator = provider.getCoordinator(context);
        coordinator.resetToCheckpoint(1L, createEmptyCheckpoint());
        coordinator.start();

        final ClassLoaderTestEnumerator enumerator = source.restoreEnumeratorFuture.get();
        assertSame(testClassLoader, enumerator.constructorClassLoader);
        assertSame(testClassLoader, enumerator.threadClassLoader.get());

        // cleanup
        coordinator.close();
    }

    @Test
    public void testSerdeBackwardCompatibility() throws Exception {
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
        assertEquals(splits, restoredEnumerator.getUnassignedSplits());
        assertTrue(restoredEnumerator.getHandledSourceEvent().isEmpty());
        assertEquals(0, restoredContext.registeredReaders().size());
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

    private void check(Runnable runnable) {
        try {
            coordinatorExecutor.submit(runnable).get();
        } catch (Exception e) {
            fail("Test failed due to " + e);
        }
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
