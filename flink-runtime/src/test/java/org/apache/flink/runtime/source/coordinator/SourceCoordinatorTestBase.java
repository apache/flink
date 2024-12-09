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
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorCheckpointSerializer;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** The test base for SourceCoordinator related tests. */
abstract class SourceCoordinatorTestBase {

    protected static final String OPERATOR_NAME = "TestOperator";
    protected static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
    protected static final int NUM_SUBTASKS = 3;

    protected boolean supportsConcurrentExecutionAttempts = false;
    private AutoCloseableRegistry closeableRegistry;

    // ---- Mocks for the underlying Operator Coordinator Context ---
    protected EventReceivingTasks receivingTasks;
    protected MockOperatorCoordinatorContext operatorCoordinatorContext;

    // ---- Mocks for the Source Coordinator Context ----
    protected String coordinatorThreadName;
    protected SplitAssignmentTracker<MockSourceSplit> splitSplitAssignmentTracker;
    protected SourceCoordinatorContext<MockSourceSplit> context;

    // ---- Mocks for the Source Coordinator ----
    protected SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> sourceCoordinator;
    private TestingSplitEnumerator<MockSourceSplit> enumerator;

    // ------------------------------------------------------------------------

    @BeforeEach
    void setup() throws Exception {
        closeableRegistry = new AutoCloseableRegistry();
        receivingTasks = EventReceivingTasks.createForRunningTasks();
        operatorCoordinatorContext =
                new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, NUM_SUBTASKS);
        splitSplitAssignmentTracker = new SplitAssignmentTracker<>();
        coordinatorThreadName = TEST_OPERATOR_ID.toHexString();

        sourceCoordinator = getNewSourceCoordinator();
        context = sourceCoordinator.getContext();
    }

    @AfterEach
    void cleanUp() throws Exception {
        closeableRegistry.close();
    }

    // ------------------------------------------------------------------------

    protected TestingSplitEnumerator<MockSourceSplit> getEnumerator() {
        if (enumerator == null) {
            enumerator =
                    (TestingSplitEnumerator<MockSourceSplit>) sourceCoordinator.getEnumerator();
            assertThat(enumerator).as("source was not started").isNotNull();
        }
        return enumerator;
    }

    protected void sourceReady() throws Exception {
        sourceCoordinator.start();
        setAllReaderTasksReady(sourceCoordinator);
    }

    protected void setAllReaderTasksReady(SourceCoordinator<?, ?> sourceCoordinator) {
        for (int i = 0; i < NUM_SUBTASKS; i++) {
            setReaderTaskReady(sourceCoordinator, i, 0);
        }
    }

    protected void setReaderTaskReady(
            SourceCoordinator<?, ?> sourceCoordinator, int subtask, int attemptNumber) {
        sourceCoordinator.executionAttemptReady(
                subtask,
                attemptNumber,
                receivingTasks.createGatewayForSubtask(subtask, attemptNumber));
    }

    protected void addTestingSplitSet(int num) {
        final List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            splits.add(new MockSourceSplit(i));
        }

        getEnumerator().addNewSplits(splits);
    }

    protected void registerReader(int subtask) {
        registerReader(subtask, 0);
    }

    protected void registerReader(int subtask, int attemptNumber) {
        sourceCoordinator.handleEventFromOperator(
                subtask,
                attemptNumber,
                new ReaderRegistrationEvent(subtask, createLocationFor(subtask, attemptNumber)));
    }

    static String createLocationFor(int subtask, int attemptNumber) {
        return String.format("location_%d_%d", subtask, attemptNumber);
    }

    protected void waitForCoordinatorToProcessActions() {
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(context);
    }

    void waitForSentEvents(int expectedEventNumber) throws Exception {
        waitUtilNumberReached(() -> receivingTasks.getNumberOfSentEvents(), expectedEventNumber);
    }

    static void waitUtilNumberReached(Supplier<Integer> numberSupplier, int expectedNumber)
            throws Exception {
        CommonTestUtils.waitUtil(
                () -> numberSupplier.get() == expectedNumber,
                Duration.ofDays(1),
                "Not reach expected number within timeout.");
    }

    static <SplitT extends SourceSplit> void assertAddSplitEvent(
            OperatorEvent event, List<SplitT> expectedSplits) throws Exception {
        assertThat(event).isInstanceOf(AddSplitEvent.class);

        final List<SplitT> splits = ((AddSplitEvent) event).splits(new MockSourceSplitSerializer());
        assertThat(splits).isEqualTo(expectedSplits);
    }

    // ------------------------------------------------------------------------

    protected SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> getNewSourceCoordinator()
            throws Exception {
        return getNewSourceCoordinator(WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED);
    }

    protected SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> getNewSourceCoordinator(
            WatermarkAlignmentParams watermarkAlignmentParams) throws Exception {
        final Source<Integer, MockSourceSplit, Set<MockSourceSplit>> mockSource =
                createMockSource();

        return new SourceCoordinator<>(
                new JobID(),
                OPERATOR_NAME,
                mockSource,
                getNewSourceCoordinatorContext(),
                new CoordinatorStoreImpl(),
                watermarkAlignmentParams,
                null);
    }

    Source<Integer, MockSourceSplit, Set<MockSourceSplit>> createMockSource() {
        return TestingSplitEnumerator.factorySource(
                new MockSourceSplitSerializer(), new MockSplitEnumeratorCheckpointSerializer());
    }

    protected SourceCoordinatorContext<MockSourceSplit> getNewSourceCoordinatorContext()
            throws Exception {
        SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, operatorCoordinatorContext);
        SourceCoordinatorContext<MockSourceSplit> coordinatorContext =
                new SourceCoordinatorContext<>(
                        new JobID(),
                        Executors.newScheduledThreadPool(1, coordinatorThreadFactory),
                        Executors.newScheduledThreadPool(
                                1, new ExecutorThreadFactory(coordinatorThreadName + "-worker")),
                        coordinatorThreadFactory,
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker,
                        supportsConcurrentExecutionAttempts);
        closeableRegistry.registerCloseable(coordinatorContext);
        return coordinatorContext;
    }
}
