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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

/** Helper class for testing {@link SourceOperator}. */
@SuppressWarnings("serial")
public class SourceOperatorTestContext implements AutoCloseable {

    public static final int SUBTASK_INDEX = 1;
    public static final MockSourceSplit MOCK_SPLIT = new MockSourceSplit(1234, 10);

    private MockSourceReader mockSourceReader;
    private MockOperatorEventGateway mockGateway;
    private SourceOperator<Integer, MockSourceSplit> operator;

    public SourceOperatorTestContext() throws Exception {
        this(false);
    }

    public SourceOperatorTestContext(boolean idle) throws Exception {
        mockSourceReader = new MockSourceReader(idle, idle);
        mockGateway = new MockOperatorEventGateway();
        operator =
                new TestingSourceOperator<>(
                        mockSourceReader,
                        mockGateway,
                        SUBTASK_INDEX,
                        true /* emit progressive watermarks */);
        Environment env = getTestingEnvironment();
        operator.setup(
                new SourceOperatorStreamTask<Integer>(env),
                new MockStreamConfig(new Configuration(), 1),
                new MockOutput<>(new ArrayList<>()));
        operator.initializeState(new StreamTaskStateInitializerImpl(env, new MemoryStateBackend()));
    }

    @Override
    public void close() throws Exception {
        operator.close();
        checkState(mockSourceReader.isClosed());
    }

    public SourceOperator<Integer, MockSourceSplit> getOperator() {
        return operator;
    }

    public MockOperatorEventGateway getGateway() {
        return mockGateway;
    }

    public MockSourceReader getSourceReader() {
        return mockSourceReader;
    }

    public StateInitializationContext createStateContext() throws Exception {
        // Create a mock split.
        byte[] serializedSplitWithVersion =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        new MockSourceSplitSerializer(), MOCK_SPLIT);

        // Crate the state context.
        OperatorStateStore operatorStateStore = createOperatorStateStore();
        StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        // Update the context.
        stateContext
                .getOperatorStateStore()
                .getListState(SourceOperator.SPLITS_STATE_DESC)
                .update(Collections.singletonList(serializedSplitWithVersion));

        return stateContext;
    }

    private OperatorStateStore createOperatorStateStore() throws Exception {
        MockEnvironment env = new MockEnvironmentBuilder().build();
        final AbstractStateBackend abstractStateBackend = new MemoryStateBackend();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        return abstractStateBackend.createOperatorStateBackend(
                env, "test-operator", Collections.emptyList(), cancelStreamRegistry);
    }

    private Environment getTestingEnvironment() {
        return new StreamMockEnvironment(
                new Configuration(),
                new Configuration(),
                new ExecutionConfig(),
                1L,
                new MockInputSplitProvider(),
                1,
                new TestTaskStateManager());
    }
}
