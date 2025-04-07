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
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link SourceCoordinatorContext}. */
class SourceCoordinatorContextTest extends SourceCoordinatorTestBase {

    @Test
    void testRegisterReader() throws Exception {
        sourceReady();
        List<ReaderInfo> readerInfo = registerReaders();

        assertThat(context.registeredReaders()).containsKey(0);
        assertThat(context.registeredReaders()).containsKey(1);
        assertThat(context.registeredReaders().get(0)).isEqualTo(readerInfo.get(0));
        assertThat(context.registeredReaders().get(1)).isEqualTo(readerInfo.get(1));

        final TestingSplitEnumerator<?> enumerator = getEnumerator();
        assertThat(enumerator.getRegisteredReaders()).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    void testTaskFailureUnregistersReader() throws Exception {
        sourceReady();
        List<ReaderInfo> readerInfo = registerReaders();

        sourceCoordinator.executionAttemptFailed(0, 0, null);
        waitForCoordinatorToProcessActions();

        assertThat(context.registeredReaders())
                .as("Only reader 2 should be registered.")
                .hasSize(2);
        assertThat(context.registeredReaders().get(0)).isNull();
        assertThat(context.registeredReaders().get(1)).isEqualTo(readerInfo.get(1));
        assertThat(context.registeredReaders().get(2)).isEqualTo(readerInfo.get(2));
    }

    @Test
    void testUnregisterUnregisteredReader() {
        context.unregisterSourceReader(0, 0);
    }

    @Test
    void testAssignSplitsFromCoordinatorExecutor() throws Exception {
        testAssignSplits(true);
    }

    @Test
    void testAssignSplitsFromOtherThread() throws Exception {
        testAssignSplits(false);
    }

    @SuppressWarnings("unchecked")
    private void testAssignSplits(boolean fromCoordinatorExecutor) throws Exception {
        sourceReady();
        registerReaders();

        // Assign splits to the readers.
        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        if (fromCoordinatorExecutor) {
            context.submitTask(() -> context.assignSplits(splitsAssignment)).get();
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
        assertThat(receivingTasks.getNumberOfSentEvents())
                .as("There should be two events sent to the subtasks.")
                .isEqualTo(2);

        // Assert the events to subtask0.
        List<OperatorEvent> eventsToSubtask0 = receivingTasks.getSentEventsForSubtask(0);
        assertThat(eventsToSubtask0).hasSize(1);
        OperatorEvent event = eventsToSubtask0.get(0);
        assertThat(event).isInstanceOf(AddSplitEvent.class);
        verifyAssignment(
                Collections.singletonList("0"),
                ((AddSplitEvent<MockSourceSplit>) event).splits(new MockSourceSplitSerializer()));
    }

    @Test
    void testAssignSplitToUnregisteredReaderFromCoordinatorExecutor() throws Exception {
        testAssignSplitToUnregisterdReader(true);
    }

    @Test
    void testAssignSplitToUnregisteredReaderFromOtherThread() throws Exception {
        testAssignSplitToUnregisterdReader(false);
    }

    private void testAssignSplitToUnregisterdReader(boolean fromCoordinatorExecutor)
            throws Exception {
        sourceReady();

        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        verifyException(
                () -> {
                    if (fromCoordinatorExecutor) {
                        context.submitTask(() -> context.assignSplits(splitsAssignment)).get();
                    } else {
                        context.assignSplits(splitsAssignment);
                    }
                },
                "assignSplits() should fail to assign the splits to a reader that is not registered.",
                "Cannot assign splits " + splitsAssignment.assignment().get(0));
    }

    @Test
    void testExceptionInRunnableFailsTheJob() throws InterruptedException, ExecutionException {
        ManuallyTriggeredScheduledExecutorService manualWorkerExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        // need the factory to have the exception handler set
        final ManuallyTriggeredScheduledExecutorService coordinatorExecutorWithExceptionHandler =
                new ManuallyTriggeredScheduledExecutorService();
        SourceCoordinatorContext<MockSourceSplit> testingContext =
                new SourceCoordinatorContext<>(
                        new JobID(),
                        coordinatorExecutorWithExceptionHandler,
                        manualWorkerExecutor,
                        new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                                coordinatorThreadName, operatorCoordinatorContext),
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker,
                        false);

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
        assertThat(operatorCoordinatorContext.isJobFailed()).isTrue();
    }

    @Test
    void testCallableInterruptedDuringShutdownDoNotFailJob() throws InterruptedException {
        AtomicReference<Throwable> expectedError = new AtomicReference<>(null);

        ManuallyTriggeredScheduledExecutorService manualWorkerExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        ManuallyTriggeredScheduledExecutorService manualCoordinatorExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        SourceCoordinatorContext<MockSourceSplit> testingContext =
                new SourceCoordinatorContext<>(
                        new JobID(),
                        manualCoordinatorExecutor,
                        manualWorkerExecutor,
                        new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                                TEST_OPERATOR_ID.toHexString(), operatorCoordinatorContext),
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker,
                        false);

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

        assertThat(expectedError.get()).isInstanceOf(InterruptedException.class);
        assertThat(operatorCoordinatorContext.isJobFailed()).isFalse();
    }

    @Test
    void testSupportsIntermediateNoMoreSplits() throws Exception {
        sourceReady();
        registerReaders();

        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        context.assignSplits(splitsAssignment);
        context.signalIntermediateNoMoreSplits(0);
        context.signalIntermediateNoMoreSplits(1);
        assertThat(context.hasNoMoreSplits(0)).isFalse();
        assertThat(context.hasNoMoreSplits(1)).isFalse();
        assertThat(context.hasNoMoreSplits(2)).isFalse();

        context.signalNoMoreSplits(0);
        context.signalNoMoreSplits(1);
        assertThat(context.hasNoMoreSplits(0)).isTrue();
        assertThat(context.hasNoMoreSplits(1)).isTrue();
        assertThat(context.hasNoMoreSplits(2)).isFalse();
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
                    0,
                    new ReaderRegistrationEvent(info.getSubtaskId(), info.getLocation()));
        }
        waitForCoordinatorToProcessActions();

        return infos;
    }

    @Test
    void testSetIsProcessingBacklog() throws Exception {
        sourceReady();
        registerReader(0, 0);
        context.setIsProcessingBacklog(true);

        for (int i = 0; i < context.currentParallelism(); ++i) {
            final List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(i);
            assertThat(events.get(events.size() - 1)).isEqualTo(new IsProcessingBacklogEvent(true));
        }

        registerReader(1, 0);
        context.setIsProcessingBacklog(false);
        registerReader(2, 0);

        for (int i = 0; i < context.currentParallelism(); ++i) {
            final List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(i);
            assertThat(events.get(events.size() - 1))
                    .isEqualTo(new IsProcessingBacklogEvent(false));
        }
    }
}
