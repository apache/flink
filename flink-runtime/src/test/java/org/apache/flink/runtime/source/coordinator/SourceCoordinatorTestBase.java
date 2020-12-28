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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumerator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;

import org.junit.After;
import org.junit.Before;

import java.util.Set;
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

    protected ExecutorService coordinatorExecutor;
    protected MockOperatorCoordinatorContext operatorCoordinatorContext;
    protected SplitAssignmentTracker<MockSourceSplit> splitSplitAssignmentTracker;
    protected SourceCoordinatorContext<MockSourceSplit> context;
    protected SourceCoordinator<?, ?> sourceCoordinator;
    private MockSplitEnumerator enumerator;

    @Before
    public void setup() throws Exception {
        operatorCoordinatorContext =
                new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, NUM_SUBTASKS);
        splitSplitAssignmentTracker = new SplitAssignmentTracker<>();
        String coordinatorThreadName = TEST_OPERATOR_ID.toHexString();
        SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, getClass().getClassLoader());

        coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        context =
                new SourceCoordinatorContext<>(
                        coordinatorExecutor,
                        coordinatorThreadFactory,
                        1,
                        operatorCoordinatorContext,
                        new MockSourceSplitSerializer(),
                        splitSplitAssignmentTracker);
        sourceCoordinator = getNewSourceCoordinator();
    }

    @After
    public void cleanUp() throws InterruptedException, TimeoutException {
        coordinatorExecutor.shutdown();
        if (!coordinatorExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new TimeoutException("Failed to close the CoordinatorExecutor before timeout.");
        }
    }

    protected MockSplitEnumerator getEnumerator() {
        if (enumerator == null) {
            enumerator = (MockSplitEnumerator) sourceCoordinator.getEnumerator();
            assertNotNull("source was not started", enumerator);
        }
        return enumerator;
    }

    // --------------------------

    protected SourceCoordinator getNewSourceCoordinator() throws Exception {
        Source<Integer, MockSourceSplit, Set<MockSourceSplit>> mockSource =
                new MockSource(Boundedness.BOUNDED, NUM_SUBTASKS * 2);

        return new SourceCoordinator<>(OPERATOR_NAME, coordinatorExecutor, mockSource, context);
    }
}
