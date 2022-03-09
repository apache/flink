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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link SourceCoordinatorContext}. */
public class SourceCoordinatorContextTest extends SourceCoordinatorTestBase {

    @Test
    public void testRegisterReader() throws Exception {
        sourceReady();
        List<ReaderInfo> readerInfo = registerReaders();

        assertTrue(context.registeredReaders().containsKey(0));
        assertTrue(context.registeredReaders().containsKey(1));
        assertEquals(readerInfo.get(0), context.registeredReaders().get(0));
        assertEquals(readerInfo.get(1), context.registeredReaders().get(1));

        final TestingSplitEnumerator<?> enumerator = getEnumerator();
        assertThat(enumerator.getRegisteredReaders(), Matchers.containsInAnyOrder(0, 1, 2));
    }

    @Test
    public void testTaskFailureUnregistersReader() throws Exception {
        sourceReady();
        List<ReaderInfo> readerInfo = registerReaders();

        sourceCoordinator.subtaskFailed(0, null);
        waitForCoordinatorToProcessActions();

        assertEquals("Only reader 2 should be registered.", 2, context.registeredReaders().size());
        assertNull(context.registeredReaders().get(0));
        assertEquals(readerInfo.get(1), context.registeredReaders().get(1));
        assertEquals(readerInfo.get(2), context.registeredReaders().get(2));
    }

    @Test
    public void testUnregisterUnregisteredReader() {
        context.unregisterSourceReader(0);
    }

    @Test
    public void testAssignSplitsFromCoordinatorExecutor() throws Exception {
        testAssignSplits(true);
    }

    @Test
    public void testAssignSplitsFromOtherThread() throws Exception {
        testAssignSplits(false);
    }

    @SuppressWarnings("unchecked")
    private void testAssignSplits(boolean fromCoordinatorExecutor) throws Exception {
        sourceReady();
        registerReaders();

        // Assign splits to the readers.
        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        if (fromCoordinatorExecutor) {
            coordinatorExecutor.submit(() -> context.assignSplits(splitsAssignment)).get();
        } else {
            context.assignSplits(splitsAssignment);
        }

        // The tracker should have recorded the assignments.
        verifyAssignment(
                Collections.singletonList("0"),
                splitSplitAssignmentTracker.uncheckpointedAssignments().get(0));
        verifyAssignment(
                Arrays.asList("1", "2"),
                splitSplitAssignmentTracker.uncheckpointedAssignments().get(1));
        // The OperatorCoordinatorContext should have received the event sending call.
        assertEquals(
                "There should be two events sent to the subtasks.",
                2,
                receivingTasks.getNumberOfSentEvents());

        // Assert the events to subtask0.
        List<OperatorEvent> eventsToSubtask0 = receivingTasks.getSentEventsForSubtask(0);
        assertEquals(1, eventsToSubtask0.size());
        OperatorEvent event = eventsToSubtask0.get(0);
        assertTrue(event instanceof AddSplitEvent);
        verifyAssignment(
                Collections.singletonList("0"),
                ((AddSplitEvent<MockSourceSplit>) event).splits(new MockSourceSplitSerializer()));
    }

    @Test
    public void testAssignSplitToUnregisteredReaderFromCoordinatorExecutor() throws Exception {
        testAssignSplitToUnregisterdReader(true);
    }

    @Test
    public void testAssignSplitToUnregisteredReaderFromOtherThread() throws Exception {
        testAssignSplitToUnregisterdReader(false);
    }

    private void testAssignSplitToUnregisterdReader(boolean fromCoordinatorExecutor)
            throws Exception {
        sourceReady();

        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        verifyException(
                () -> {
                    if (fromCoordinatorExecutor) {
                        coordinatorExecutor
                                .submit(() -> context.assignSplits(splitsAssignment))
                                .get();
                    } else {
                        context.assignSplits(splitsAssignment);
                    }
                },
                "assignSplits() should fail to assign the splits to a reader that is not registered.",
                "Cannot assign splits");
    }

    @Test
    public void testExceptionInRunnableFailsTheJob()
            throws InterruptedException, ExecutionException {
        ManuallyTriggeredScheduledExecutorService manualWorkerExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        // need the factory to have the exception handler set
        final ManuallyTriggeredScheduledExecutorService coordinatorExecutorWithExceptionHandler =
                new ManuallyTriggeredScheduledExecutorService();
        SourceCoordinatorContext<MockSourceSplit> testingContext =
                new SourceCoordinatorContext<>(
                        coordinatorExecutorWithExceptionHandler,
                        manualWorkerExecutor,
                        coordinatorThreadFactory,
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker);

        testingContext.runInCoordinatorThread(
                () -> {
                    throw new RuntimeException();
                });

        manualWorkerExecutor.triggerAll();
        // shutdown coordinatorExecutor and blocks until tasks are finished
        testingContext.close();
        coordinatorExecutorWithExceptionHandler.triggerAll();
        // blocks until the job is failed: wait that the uncaught exception handler calls
        // operatorCoordinatorContext#failJob() which completes the future
        operatorCoordinatorContext.getJobFailedFuture().get();
        assertTrue(operatorCoordinatorContext.isJobFailed());
    }

    @Test
    public void testCallableInterruptedDuringShutdownDoNotFailJob() throws InterruptedException {
        AtomicReference<Throwable> expectedError = new AtomicReference<>(null);

        ManuallyTriggeredScheduledExecutorService manualWorkerExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        ManuallyTriggeredScheduledExecutorService manualCoordinatorExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        SourceCoordinatorContext<MockSourceSplit> testingContext =
                new SourceCoordinatorContext<>(
                        manualCoordinatorExecutor,
                        manualWorkerExecutor,
                        new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                                TEST_OPERATOR_ID.toHexString(), operatorCoordinatorContext),
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker);

        testingContext.callAsync(
                () -> {
                    throw new InterruptedException();
                },
                (ignored, e) -> {
                    if (e != null) {
                        expectedError.set(e);
                        throw new RuntimeException(e);
                    }
                });

        manualWorkerExecutor.triggerAll();
        testingContext.close();
        manualCoordinatorExecutor.triggerAll();

        assertTrue(expectedError.get() instanceof InterruptedException);
        assertFalse(operatorCoordinatorContext.isJobFailed());
    }

    // ------------------------

    private List<ReaderInfo> registerReaders() {
        final List<ReaderInfo> infos =
                Arrays.asList(
                        new ReaderInfo(0, "subtask_0_location"),
                        new ReaderInfo(1, "subtask_1_location"),
                        new ReaderInfo(2, "subtask_2_location"));

        for (ReaderInfo info : infos) {
            sourceCoordinator.handleEventFromOperator(
                    info.getSubtaskId(),
                    new ReaderRegistrationEvent(info.getSubtaskId(), info.getLocation()));
        }
        waitForCoordinatorToProcessActions();

        return infos;
    }
}
