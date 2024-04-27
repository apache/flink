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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TestingCheckpointScheduling;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code StopWithSavepointTerminationHandlerImplTest} tests {@link
 * StopWithSavepointTerminationHandlerImpl}.
 */
class StopWithSavepointTerminationHandlerImplTest {

    private static final Logger log =
            LoggerFactory.getLogger(StopWithSavepointTerminationHandlerImplTest.class);

    private static final JobID JOB_ID = new JobID();

    private final TestingCheckpointScheduling checkpointScheduling =
            new TestingCheckpointScheduling(false);

    private StopWithSavepointTerminationHandlerImpl createTestInstanceFailingOnGlobalFailOver() {
        return createTestInstance(
                throwableCausingGlobalFailOver -> {
                    throw new AssertionError("No global failover should be triggered.");
                });
    }

    private StopWithSavepointTerminationHandlerImpl createTestInstance(
            Consumer<Throwable> handleGlobalFailureConsumer) {
        // checkpointing should be always stopped before initiating stop-with-savepoint
        checkpointScheduling.stopCheckpointScheduler();

        final SchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setHandleGlobalFailureConsumer(handleGlobalFailureConsumer)
                        .build();
        return new StopWithSavepointTerminationHandlerImpl(
                JOB_ID, scheduler, checkpointScheduling, log);
    }

    @Test
    void testHappyPath() throws ExecutionException, InterruptedException {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstanceFailingOnGlobalFailOver();

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint = createCompletedSavepoint(streamStateHandle);
        testInstance.handleSavepointCreation(completedSavepoint, null);
        testInstance.handleExecutionsTermination(Collections.singleton(ExecutionState.FINISHED));

        assertThatFuture(testInstance.getSavepointPath())
                .isCompletedWithValue(completedSavepoint.getExternalPointer());

        assertThat(streamStateHandle.isDisposed())
                .withFailMessage("The savepoint should not have been discarded.")
                .isFalse();
        assertThat(checkpointScheduling.isEnabled())
                .withFailMessage("Checkpoint scheduling should be disabled.")
                .isFalse();
    }

    @Test
    void testSavepointCreationFailureWithoutExecutionTermination() {
        // savepoint creation failure is handled as expected if no execution termination happens
        assertSavepointCreationFailure(testInstance -> {});
    }

    @Test
    void testSavepointCreationFailureWithFailingExecutions() {
        // no global fail-over is expected to be triggered by the stop-with-savepoint despite the
        // execution failure
        assertSavepointCreationFailure(
                testInstance ->
                        testInstance.handleExecutionsTermination(
                                Collections.singletonList(ExecutionState.FAILED)));
    }

    @Test
    void testSavepointCreationFailureWithFinishingExecutions() {
        // checkpoint scheduling should be still enabled despite the finished executions
        assertSavepointCreationFailure(
                testInstance ->
                        testInstance.handleExecutionsTermination(
                                Collections.singletonList(ExecutionState.FINISHED)));
    }

    public void assertSavepointCreationFailure(
            Consumer<StopWithSavepointTerminationHandler> handleExecutionsTermination) {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstanceFailingOnGlobalFailOver();

        final String expectedErrorMessage = "Expected exception during savepoint creation.";
        testInstance.handleSavepointCreation(null, new Exception(expectedErrorMessage));
        handleExecutionsTermination.accept(testInstance);

        assertThatThrownBy(() -> testInstance.getSavepointPath().get())
                .withFailMessage("An ExecutionException is expected.")
                .isInstanceOf(Throwable.class)
                .hasMessageContaining(expectedErrorMessage);

        // the checkpoint scheduling should be enabled in case of failure
        assertThat(checkpointScheduling.isEnabled())
                .withFailMessage("Checkpoint scheduling should be enabled.")
                .isTrue();
    }

    @Test
    void testFailedTerminationHandling() {
        final CompletableFuture<Throwable> globalFailOverTriggered = new CompletableFuture<>();
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstance(globalFailOverTriggered::complete);

        final ExecutionState expectedNonFinishedState = ExecutionState.FAILED;

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint = createCompletedSavepoint(streamStateHandle);

        testInstance.handleSavepointCreation(completedSavepoint, null);
        testInstance.handleExecutionsTermination(
                Collections.singletonList(expectedNonFinishedState));

        assertThatThrownBy(() -> testInstance.getSavepointPath().get())
                .withFailMessage("An ExecutionException is expected.")
                .isInstanceOf(Throwable.class)
                .hasCauseInstanceOf(StopWithSavepointStoppingException.class);

        assertThatFuture(globalFailOverTriggered)
                .withFailMessage("Global fail-over was not triggered.")
                .isDone();
        assertThat(streamStateHandle.isDisposed())
                .withFailMessage("Savepoint should not be discarded.")
                .isFalse();
    }

    @Test
    void testInvalidExecutionTerminationCall() {
        assertThatThrownBy(
                        () ->
                                createTestInstanceFailingOnGlobalFailOver()
                                        .handleExecutionsTermination(
                                                Collections.singletonList(ExecutionState.FINISHED)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSavepointCreationParameterBothNull() {
        assertThatThrownBy(
                        () ->
                                createTestInstanceFailingOnGlobalFailOver()
                                        .handleSavepointCreation(null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSavepointCreationParameterBothSet() {
        assertThatThrownBy(
                        () ->
                                createTestInstanceFailingOnGlobalFailOver()
                                        .handleSavepointCreation(
                                                createCompletedSavepoint(
                                                        new EmptyStreamStateHandle()),
                                                new Exception(
                                                        "No exception should be passed if a savepoint is available.")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testExecutionTerminationWithNull() {
        assertThatThrownBy(
                        () ->
                                createTestInstanceFailingOnGlobalFailOver()
                                        .handleExecutionsTermination(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static CompletedCheckpoint createCompletedSavepoint(
            StreamStateHandle streamStateHandle) {
        return new CompletedCheckpoint(
                JOB_ID,
                0,
                0L,
                0L,
                new HashMap<>(),
                null,
                CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL),
                new TestCompletedCheckpointStorageLocation(streamStateHandle, "savepoint-path"),
                null);
    }
}
