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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for basic {@link CompletedCheckpointStore} contract. */
abstract class CompletedCheckpointStoreTest {

    /** Creates the {@link CompletedCheckpointStore} implementation to be tested. */
    protected abstract CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain, Executor executor) throws Exception;

    protected CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain) throws Exception {
        return createRecoveredCompletedCheckpointStore(
                maxNumberOfCheckpointsToRetain, Executors.directExecutor());
    }

    // ---------------------------------------------------------------------------------------------

    /** Tests that at least one checkpoint needs to be retained. */
    @Test
    void testExceptionOnNoRetainedCheckpoints() {
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> createRecoveredCompletedCheckpointStore(0));
    }

    /** Tests adding and getting a checkpoint. */
    @Test
    void testAddAndGetLatestCheckpoint() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(4);

        // Empty state
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isZero();
        assertThat(checkpoints.getAllCheckpoints()).isEmpty();

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                    createCheckpoint(1, sharedStateRegistry)
                };

        // Add and get latest
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[0], new CheckpointsCleaner(), () -> {});
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isOne();
        verifyCheckpoint(expected[0], checkpoints.getLatestCheckpoint());

        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[1], new CheckpointsCleaner(), () -> {});
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isEqualTo(2);
        verifyCheckpoint(expected[1], checkpoints.getLatestCheckpoint());
    }

    /**
     * Tests that adding more checkpoints than retained discards the correct checkpoints (using the
     * correct class loader).
     */
    @Test
    void testAddCheckpointMoreThanMaxRetained() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(1);
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                            createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry),
                            createCheckpoint(3, sharedStateRegistry)
                };

        // Add checkpoints
        checkpoints.addCheckpointAndSubsumeOldestOne(expected[0], checkpointsCleaner, () -> {});
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isOne();

        for (int i = 1; i < expected.length; i++) {
            checkpoints.addCheckpointAndSubsumeOldestOne(expected[i], checkpointsCleaner, () -> {});

            // The ZooKeeper implementation discards asynchronously
            expected[i - 1].awaitDiscard();
            assertThat(expected[i - 1].isDiscarded()).isTrue();
            assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isOne();
        }
    }

    /**
     * Tests that
     *
     * <ul>
     *   <li>{@link CompletedCheckpointStore#getLatestCheckpoint()} returns <code>null</code> ,
     *   <li>{@link CompletedCheckpointStore#getAllCheckpoints()} returns an empty list,
     *   <li>{@link CompletedCheckpointStore#getNumberOfRetainedCheckpoints()} returns 0.
     * </ul>
     */
    @Test
    void testEmptyState() throws Exception {
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(1);

        assertThat(checkpoints.getLatestCheckpoint()).isNull();
        assertThat(checkpoints.getAllCheckpoints()).isEmpty();
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isZero();
    }

    /** Tests that all added checkpoints are returned. */
    @Test
    void testGetAllCheckpoints() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(4);

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                            createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry),
                            createCheckpoint(3, sharedStateRegistry)
                };

        for (TestCompletedCheckpoint checkpoint : expected) {
            checkpoints.addCheckpointAndSubsumeOldestOne(
                    checkpoint, new CheckpointsCleaner(), () -> {});
        }

        List<CompletedCheckpoint> actual = checkpoints.getAllCheckpoints();

        assertThat(actual).hasSameSizeAs(expected).containsExactly(expected);
    }

    /** Tests that all checkpoints are discarded (using the correct class loader). */
    @Test
    void testDiscardAllCheckpoints() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(4);

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                            createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry),
                            createCheckpoint(3, sharedStateRegistry)
                };

        for (TestCompletedCheckpoint checkpoint : expected) {
            checkpoints.addCheckpointAndSubsumeOldestOne(
                    checkpoint, new CheckpointsCleaner(), () -> {});
        }

        checkpoints.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());

        // Empty state
        assertThat(checkpoints.getLatestCheckpoint()).isNull();
        assertThat(checkpoints.getAllCheckpoints()).isEmpty();
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isZero();

        // All have been discarded
        for (TestCompletedCheckpoint checkpoint : expected) {
            // The ZooKeeper implementation discards asynchronously
            checkpoint.awaitDiscard();
            assertThat(checkpoint.isDiscarded()).isTrue();
        }
    }

    @Test
    void testAcquireLatestCompletedCheckpointId() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(1);
        assertThat(checkpoints.getLatestCheckpointId()).isZero();

        checkpoints.addCheckpointAndSubsumeOldestOne(
                createCheckpoint(2, sharedStateRegistry), new CheckpointsCleaner(), () -> {});
        assertThat(checkpoints.getLatestCheckpointId()).isEqualTo(2);

        checkpoints.addCheckpointAndSubsumeOldestOne(
                createCheckpoint(4, sharedStateRegistry), new CheckpointsCleaner(), () -> {});
        assertThat(checkpoints.getLatestCheckpointId()).isEqualTo(4);
    }

    // ---------------------------------------------------------------------------------------------

    public static TestCompletedCheckpoint createCheckpoint(
            long id, SharedStateRegistry sharedStateRegistry) {
        return createCheckpoint(
                id,
                sharedStateRegistry,
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION));
    }

    public static TestCompletedCheckpoint createCheckpoint(
            long id, SharedStateRegistry sharedStateRegistry, CheckpointProperties props) {

        int numberOfStates = 4;

        OperatorID operatorID = new OperatorID();

        Map<OperatorID, OperatorState> operatorGroupState = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorID, numberOfStates, numberOfStates);
        operatorGroupState.put(operatorID, operatorState);

        for (int i = 0; i < numberOfStates; i++) {
            OperatorSubtaskState subtaskState = new TestOperatorSubtaskState();

            operatorState.putState(i, subtaskState);
        }

        operatorState.registerSharedStates(sharedStateRegistry, id);

        return new TestCompletedCheckpoint(new JobID(), id, 0, operatorGroupState, props);
    }

    protected void verifyCheckpointRegistered(Collection<OperatorState> operatorStates) {
        for (OperatorState operatorState : operatorStates) {
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                assertThat(((TestOperatorSubtaskState) subtaskState).registered).isTrue();
            }
        }
    }

    public static void verifyCheckpointDiscarded(TestCompletedCheckpoint completedCheckpoint) {
        assertThat(completedCheckpoint.isDiscarded()).isTrue();
        verifyCheckpointDiscarded(completedCheckpoint.getOperatorStates().values());
    }

    protected static void verifyCheckpointDiscarded(Collection<OperatorState> operatorStates) {
        for (OperatorState operatorState : operatorStates) {
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                assertThat(((TestOperatorSubtaskState) subtaskState).discarded).isTrue();
            }
        }
    }

    private void verifyCheckpoint(CompletedCheckpoint expected, CompletedCheckpoint actual) {
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * A test {@link CompletedCheckpoint}. We want to verify that the correct class loader is used
     * when discarding. Spying on a regular {@link CompletedCheckpoint} instance with Mockito
     * doesn't work, because it breaks serializability.
     */
    protected static class TestCompletedCheckpoint extends CompletedCheckpoint {

        private static final long serialVersionUID = 4211419809665983026L;

        private boolean isDiscarded;

        // Latch for test variants which discard asynchronously
        private final transient CountDownLatch discardLatch = new CountDownLatch(1);

        public TestCompletedCheckpoint(
                JobID jobId,
                long checkpointId,
                long timestamp,
                Map<OperatorID, OperatorState> operatorGroupState,
                CheckpointProperties props) {

            super(
                    jobId,
                    checkpointId,
                    timestamp,
                    Long.MAX_VALUE,
                    operatorGroupState,
                    null,
                    props,
                    new TestCompletedCheckpointStorageLocation(),
                    null);
        }

        @Override
        public CompletedCheckpointDiscardObject markAsDiscarded() {
            return new TestCompletedCheckpointDiscardObject();
        }

        public boolean isDiscarded() {
            return isDiscarded;
        }

        public void awaitDiscard() throws InterruptedException {
            if (discardLatch != null) {
                discardLatch.await();
            }
        }

        public boolean awaitDiscard(long timeout) throws InterruptedException {
            if (discardLatch != null) {
                return discardLatch.await(timeout, TimeUnit.MILLISECONDS);
            } else {
                return false;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestCompletedCheckpoint that = (TestCompletedCheckpoint) o;

            return getJobId().equals(that.getJobId())
                    && getCheckpointID() == that.getCheckpointID();
        }

        @Override
        public int hashCode() {
            return getJobId().hashCode() + (int) getCheckpointID();
        }

        /** */
        public class TestCompletedCheckpointDiscardObject extends CompletedCheckpointDiscardObject {
            @Override
            public void discard() throws Exception {
                super.discard();
                updateDiscards();
            }

            @Override
            public CompletableFuture<Void> discardAsync(Executor executor) {
                return super.discardAsync(executor).thenRun(this::updateDiscards);
            }

            private void updateDiscards() {
                if (!isDiscarded) {
                    isDiscarded = true;

                    if (discardLatch != null) {
                        discardLatch.countDown();
                    }
                }
            }
        }
    }

    public static class TestOperatorSubtaskState extends OperatorSubtaskState {
        private static final long serialVersionUID = 522580433699164230L;

        boolean registered;
        boolean discarded;

        public TestOperatorSubtaskState() {
            super();
            this.registered = false;
            this.discarded = false;
        }

        @Override
        public void discardState() {
            super.discardState();
            assertThat(discarded).isFalse();
            discarded = true;
            registered = false;
        }

        @Override
        public void registerSharedStates(
                SharedStateRegistry sharedStateRegistry, long checkpointID) {
            super.registerSharedStates(sharedStateRegistry, checkpointID);
            assertThat(discarded).isFalse();
            registered = true;
        }

        public void reset() {
            registered = false;
            discarded = false;
        }

        public boolean isRegistered() {
            return registered;
        }

        public boolean isDiscarded() {
            return discarded;
        }
    }
}
