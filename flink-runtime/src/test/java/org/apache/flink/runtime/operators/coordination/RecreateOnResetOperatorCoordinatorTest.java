/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link RecreateOnResetOperatorCoordinator}. */
public class RecreateOnResetOperatorCoordinatorTest {
    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final int NUM_SUBTASKS = 1;

    @Test
    public void testQuiesceableContextNotQuiesced() throws TaskNotRunningException {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        TestingEvent event = new TestingEvent();
        quiesceableContext.sendEvent(event, 0);
        quiesceableContext.failJob(new Exception());

        assertEquals(OPERATOR_ID, quiesceableContext.getOperatorId());
        assertEquals(NUM_SUBTASKS, quiesceableContext.currentParallelism());
        assertEquals(Collections.singletonList(event), context.getEventsToOperatorBySubtaskId(0));
        assertTrue(context.isJobFailed());
    }

    @Test
    public void testQuiescedContext() throws TaskNotRunningException {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        quiesceableContext.quiesce();
        quiesceableContext.sendEvent(new TestingEvent(), 0);
        quiesceableContext.failJob(new Exception());

        assertEquals(OPERATOR_ID, quiesceableContext.getOperatorId());
        assertEquals(NUM_SUBTASKS, quiesceableContext.currentParallelism());
        assertTrue(context.getEventsToOperator().isEmpty());
        assertFalse(context.isJobFailed());
    }

    @Test
    public void testResetToCheckpoint() throws Exception {
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        RecreateOnResetOperatorCoordinator.QuiesceableContext contextBeforeReset =
                coordinator.getQuiesceableContext();
        TestingOperatorCoordinator internalCoordinatorBeforeReset =
                getInternalCoordinator(coordinator);

        byte[] stateToRestore = new byte[0];
        coordinator.resetToCheckpoint(1L, stateToRestore);

        // Use the checkpoint to ensure all the previous method invocation has succeeded.
        coordinator.waitForAllAsyncCallsFinish();

        assertTrue(contextBeforeReset.isQuiesced());
        assertNull(internalCoordinatorBeforeReset.getLastRestoredCheckpointState());

        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        assertEquals(
                stateToRestore, internalCoordinatorAfterReset.getLastRestoredCheckpointState());
    }

    @Test
    public void testResetToCheckpointTimeout() throws Exception {
        final long closingTimeoutMs = 1L;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(new CountDownLatch(1));
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        coordinator.resetToCheckpoint(2L, new byte[0]);
        CommonTestUtils.waitUtil(
                context::isJobFailed,
                Duration.ofSeconds(5),
                "The job should fail due to resetToCheckpoint() timeout.");
    }

    @Test
    public void testMethodCallsOnLongResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final CountDownLatch blockOnCloseLatch = new CountDownLatch(1);
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(blockOnCloseLatch);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Set up the testing variables.
        final byte[] restoredState = new byte[0];
        final TestingEvent testingEvent = new TestingEvent();
        final long completedCheckpointId = 1234L;

        // Reset the coordinator which closes the current internal coordinator
        // and then create a new one. The closing of the current internal
        // coordinator will block until the blockOnCloseLatch is pulled.
        coordinator.resetToCheckpoint(2L, restoredState);

        // The following method calls should be applied to the new internal
        // coordinator asynchronously because the current coordinator has not
        // been successfully closed yet.
        coordinator.handleEventFromOperator(1, testingEvent);
        coordinator.subtaskFailed(1, new Exception("Subtask Failure Exception."));
        coordinator.notifyCheckpointComplete(completedCheckpointId);

        // The new coordinator should not have been created because the resetToCheckpoint()
        // should block on closing the current coordinator.
        assertEquals(1, provider.getCreatedCoordinators().size());

        // Now unblock the closing of the current coordinator.
        blockOnCloseLatch.countDown();

        // Take a checkpoint on the coordinator after reset.
        CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        coordinator.checkpointCoordinator(5678L, checkpointFuture);
        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the new coordinator.
        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        // The internal coordinator after reset should have triggered a new checkpoint.
        assertEquals(checkpointFuture, internalCoordinatorAfterReset.getLastTriggeredCheckpoint());
        // The internal coordinator after reset should be the second coordinator created by the
        // provider.
        assertEquals(provider.getCreatedCoordinators().get(1), internalCoordinatorAfterReset);
        // The internal coordinator after reset should have been reset to the restored state.
        assertEquals(restoredState, internalCoordinatorAfterReset.getLastRestoredCheckpointState());
        // The internal coordinator after reset should have received the testing event.
        assertEquals(testingEvent, internalCoordinatorAfterReset.getNextReceivedOperatorEvent());
        // The internal coordinator after reset should have handled the failure of subtask 1.
        assertEquals(Collections.singletonList(1), internalCoordinatorAfterReset.getFailedTasks());
        // The internal coordinator after reset should have the completedCheckpointId.
        assertEquals(
                completedCheckpointId, internalCoordinatorAfterReset.getLastCheckpointComplete());
    }

    @Test(timeout = 30000L)
    public void testConsecutiveResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final int numResets = 1000;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider();
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Loop to get some interleaved method invocations on multiple instances
        // of active coordinators.
        for (int i = 0; i < numResets; i++) {
            coordinator.handleEventFromOperator(1, new TestingEvent(i));
            coordinator.subtaskFailed(i, new Exception());
            CompletableFuture<byte[]> future = CompletableFuture.completedFuture(new byte[i]);
            coordinator.checkpointCoordinator(i, future);
            final int loop = i;
            future.thenRun(() -> coordinator.notifyCheckpointComplete(loop));
            // The reset bytes has a length of i+1 here because this will be reset to the
            // next internal coordinator.
            coordinator.resetToCheckpoint(i, new byte[i + 1]);
        }

        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the coordinators.
        for (TestingOperatorCoordinator internalCoordinator : provider.getCreatedCoordinators()) {
            // The indexOfCoordinator is set to 0 by default because:
            // 1. For the initial internal coordinator, its index is 0.
            // 2. For all the subsequent internal coordinators, there are two cases:
            //    a. they have processed at least one method call. In that case the coordinator
            //       must have been restored to the given state. So the indexOfCoordinator will
            //       be updated correctly.
            //    b. no method call was processed. In this case the indexOfCoordinator does not
            //       matter because all the fields will either be empty or null.
            int indexOfCoordinator = 0;
            byte[] lastRestoredState = internalCoordinator.getLastRestoredCheckpointState();
            if (lastRestoredState != null) {
                indexOfCoordinator = lastRestoredState.length;
            }
            TestingEvent testingEvent =
                    (TestingEvent) internalCoordinator.getNextReceivedOperatorEvent();
            List<Integer> failedTasks = internalCoordinator.getFailedTasks();

            assertTrue(testingEvent == null || testingEvent.getId() == indexOfCoordinator);
            assertTrue(
                    failedTasks.isEmpty()
                            || (failedTasks.size() == 1
                                    && failedTasks.get(0) == indexOfCoordinator));
            assertTrue(
                    !internalCoordinator.hasCompleteCheckpoint()
                            || internalCoordinator.getLastCheckpointComplete()
                                    == indexOfCoordinator);
            assertTrue(
                    !internalCoordinator.hasTriggeredCheckpoint()
                            || internalCoordinator.getLastTriggeredCheckpoint().get().length
                                    == indexOfCoordinator);
        }
        coordinator.close();
        TestingOperatorCoordinator internalCoordinator = getInternalCoordinator(coordinator);
        CommonTestUtils.waitUtil(
                internalCoordinator::isClosed,
                Duration.ofSeconds(5),
                "Timed out when waiting for the coordinator to close.");
    }

    public void testFailureInCreateCoordinator() {}

    // ---------------

    private RecreateOnResetOperatorCoordinator createCoordinator(
            TestingCoordinatorProvider provider, OperatorCoordinator.Context context)
            throws Exception {
        return (RecreateOnResetOperatorCoordinator) provider.create(context);
    }

    private TestingOperatorCoordinator getInternalCoordinator(
            RecreateOnResetOperatorCoordinator coordinator) throws Exception {
        return (TestingOperatorCoordinator) coordinator.getInternalCoordinator();
    }

    // ---------------

    private static class TestingCoordinatorProvider
            extends RecreateOnResetOperatorCoordinator.Provider {
        private static final long serialVersionUID = 4184184580789587013L;
        private final CountDownLatch blockOnCloseLatch;
        private final List<TestingOperatorCoordinator> createdCoordinators;

        public TestingCoordinatorProvider() {
            this(null);
        }

        public TestingCoordinatorProvider(CountDownLatch blockOnCloseLatch) {
            super(OPERATOR_ID);
            this.blockOnCloseLatch = blockOnCloseLatch;
            this.createdCoordinators = new ArrayList<>();
        }

        @Override
        protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            TestingOperatorCoordinator testingCoordinator =
                    new TestingOperatorCoordinator(context, blockOnCloseLatch);
            createdCoordinators.add(testingCoordinator);
            return testingCoordinator;
        }

        private List<TestingOperatorCoordinator> getCreatedCoordinators() {
            return createdCoordinators;
        }
    }

    private static class TestingEvent implements OperatorEvent {
        private static final long serialVersionUID = -3289352911927668275L;
        private final int id;

        private TestingEvent() {
            this(-1);
        }

        private TestingEvent(int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }
    }
}
