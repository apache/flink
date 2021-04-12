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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorCheckpointSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;

/** The test base for SourceCoordinator related tests. */
public abstract class SourceCoordinatorTestBase {

    protected static final String OPERATOR_NAME = "TestOperator";
    protected static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
    protected static final int NUM_SUBTASKS = 3;

    // ---- Mocks for the underlying Operator Coordinator Context ---
    protected EventReceivingTasks receivingTasks;
    protected MockOperatorCoordinatorContext operatorCoordinatorContext;

    // ---- Mocks for the Source Coordinator Context ----
    protected SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    protected ExecutorService coordinatorExecutor;
    protected SplitAssignmentTracker<MockSourceSplit> splitSplitAssignmentTracker;
    protected SourceCoordinatorContext<MockSourceSplit> context;

    // ---- Mocks for the Source Coordinator ----
    protected SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> sourceCoordinator;
    private TestingSplitEnumerator<MockSourceSplit> enumerator;

    // ------------------------------------------------------------------------

    @Before
    public void setup() throws Exception {
        receivingTasks = EventReceivingTasks.createForRunningTasks();
        operatorCoordinatorContext =
                new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, NUM_SUBTASKS);
        splitSplitAssignmentTracker = new SplitAssignmentTracker<>();
        String coordinatorThreadName = TEST_OPERATOR_ID.toHexString();
        coordinatorThreadFactory =
                new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, getClass().getClassLoader());

        coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        sourceCoordinator = getNewSourceCoordinator();
        context = sourceCoordinator.getContext();
    }

    @After
    public void cleanUp() throws InterruptedException, TimeoutException {
        coordinatorExecutor.shutdown();
        if (!coordinatorExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new TimeoutException("Failed to close the CoordinatorExecutor before timeout.");
        }
    }

    // ------------------------------------------------------------------------

    protected TestingSplitEnumerator<MockSourceSplit> getEnumerator() {
        if (enumerator == null) {
            enumerator =
                    (TestingSplitEnumerator<MockSourceSplit>) sourceCoordinator.getEnumerator();
            assertNotNull("source was not started", enumerator);
        }
        return enumerator;
    }

    protected void sourceReady() throws Exception {
        sourceCoordinator.start();
        setAllReaderTasksReady(sourceCoordinator);
    }

    protected void setAllReaderTasksReady() {
        setAllReaderTasksReady(sourceCoordinator);
    }

    protected void setAllReaderTasksReady(SourceCoordinator<?, ?> sourceCoordinator) {
        for (int i = 0; i < NUM_SUBTASKS; i++) {
            sourceCoordinator.subtaskReady(i, receivingTasks.createGatewayForSubtask(i));
        }
    }

    protected void addTestingSplitSet(int num) {
        final List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            splits.add(new MockSourceSplit(i));
        }

        getEnumerator().addNewSplits(splits);
    }

    protected void registerReader(int subtask) {
        sourceCoordinator.handleEventFromOperator(
                subtask, new ReaderRegistrationEvent(subtask, "location_" + subtask));
    }

    protected void waitForCoordinatorToProcessActions() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        context.runInCoordinatorThread(() -> future.complete(null));

        try {
            future.get();
        } catch (InterruptedException e) {
            throw new AssertionError("test interrupted");
        } catch (ExecutionException e) {
            ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
        }
    }

    // ------------------------------------------------------------------------

    protected SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> getNewSourceCoordinator() {
        final Source<Integer, MockSourceSplit, Set<MockSourceSplit>> mockSource =
                TestingSplitEnumerator.factorySource(
                        new MockSourceSplitSerializer(),
                        new MockSplitEnumeratorCheckpointSerializer());

        return new SourceCoordinator<>(
                OPERATOR_NAME, coordinatorExecutor, mockSource, getNewSourceCoordinatorContext());
    }

    protected SourceCoordinatorContext<MockSourceSplit> getNewSourceCoordinatorContext() {
        return new SourceCoordinatorContext<>(
                coordinatorExecutor,
                Executors.newScheduledThreadPool(
                        1,
                        new ExecutorThreadFactory(
                                coordinatorThreadFactory.getCoordinatorThreadName() + "-worker")),
                coordinatorThreadFactory,
                operatorCoordinatorContext,
                new MockSourceSplitSerializer(),
                splitSplitAssignmentTracker);
    }
}
