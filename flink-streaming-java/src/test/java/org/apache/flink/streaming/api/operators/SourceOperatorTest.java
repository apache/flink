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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.CollectionUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SourceOperator}.
 */
@SuppressWarnings("serial")
public class SourceOperatorTest {

	private static final int SUBTASK_INDEX = 1;
	private static final MockSourceSplit MOCK_SPLIT = new MockSourceSplit(1234, 10);

	private MockSourceReader mockSourceReader;
	private MockOperatorEventGateway mockGateway;
	private SourceOperator<Integer, MockSourceSplit> operator;

	@Before
	public void setup() throws Exception {
		this.mockSourceReader = new MockSourceReader();
		this.mockGateway = new MockOperatorEventGateway();
		this.operator = new TestingSourceOperator<>(mockSourceReader, mockGateway, SUBTASK_INDEX, true /* emit progressive watermarks */);
		Environment env = getTestingEnvironment();
		this.operator.setup(
				new SourceOperatorStreamTask<Integer>(env),
				new MockStreamConfig(new Configuration(), 1),
				new MockOutput<>(new ArrayList<>()));
		this.operator.initializeState(new StreamTaskStateInitializerImpl(env, new MemoryStateBackend()));
	}

	@After
	public void cleanUp() throws Exception {
		operator.close();
		assertTrue(mockSourceReader.isClosed());
	}

	@Test
	public void testInitializeState() throws Exception {
		StateInitializationContext stateContext = getStateContext();
		operator.initializeState(stateContext);

		assertNotNull(stateContext.getOperatorStateStore().getListState(SourceOperator.SPLITS_STATE_DESC));
	}

	@Test
	public void testOpen() throws Exception {
		// Initialize the operator.
		operator.initializeState(getStateContext());
		// Open the operator.
		operator.open();
		// The source reader should have been assigned a split.
		assertEquals(Collections.singletonList(MOCK_SPLIT), mockSourceReader.getAssignedSplits());
		// The source reader should have started.
		assertTrue(mockSourceReader.isStarted());

		// A ReaderRegistrationRequest should have been sent.
		assertEquals(1, mockGateway.getEventsSent().size());
		OperatorEvent operatorEvent = mockGateway.getEventsSent().get(0);
		assertTrue(operatorEvent instanceof ReaderRegistrationEvent);
		assertEquals(SUBTASK_INDEX, ((ReaderRegistrationEvent) operatorEvent).subtaskId());
	}

	@Test
	public void testHandleAddSplitsEvent() throws Exception {
		operator.initializeState(getStateContext());
		operator.open();
		MockSourceSplit newSplit = new MockSourceSplit((2));
		operator.handleOperatorEvent(new AddSplitEvent<>(
			Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
		// The source reader should have been assigned two splits.
		assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), mockSourceReader.getAssignedSplits());
	}

	@Test
	public void testHandleAddSourceEvent() throws Exception {
		operator.initializeState(getStateContext());
		operator.open();
		SourceEvent event = new SourceEvent() {
		};
		operator.handleOperatorEvent(new SourceEventWrapper(event));
		// The source reader should have been assigned two splits.
		assertEquals(Collections.singletonList(event), mockSourceReader.getReceivedSourceEvents());
	}

	@Test
	public void testSnapshotState() throws Exception {
		StateInitializationContext stateContext = getStateContext();
		operator.initializeState(stateContext);
		operator.open();
		MockSourceSplit newSplit = new MockSourceSplit((2));
		operator.handleOperatorEvent(new AddSplitEvent<>(
			Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
		operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));

		// Verify the splits in state.
		List<MockSourceSplit> splitsInState = CollectionUtil.iterableToList(operator.getReaderState().get());
		assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), splitsInState);
	}

	@Test
	public void testNotifyCheckpointComplete() throws Exception {
		StateInitializationContext stateContext = getStateContext();
		operator.initializeState(stateContext);
		operator.open();
		operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
		operator.notifyCheckpointComplete(100L);
		assertEquals(100L, (long) mockSourceReader.getCompletedCheckpoints().get(0));
	}

	@Test
	public void testNotifyCheckpointAborted() throws Exception {
		StateInitializationContext stateContext = getStateContext();
		operator.initializeState(stateContext);
		operator.open();
		operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
		operator.notifyCheckpointAborted(100L);
		assertEquals(100L, (long) mockSourceReader.getAbortedCheckpoints().get(0));
	}

	// ---------------- helper methods -------------------------

	private StateInitializationContext getStateContext() throws Exception {
		// Create a mock split.
		byte[] serializedSplitWithVersion = SimpleVersionedSerialization
			.writeVersionAndSerialize(new MockSourceSplitSerializer(), MOCK_SPLIT);

		// Crate the state context.
		OperatorStateStore operatorStateStore = createOperatorStateStore();
		StateInitializationContext stateContext = new StateInitializationContextImpl(
			false,
			operatorStateStore,
			null,
			null,
			null);

		// Update the context.
		stateContext.getOperatorStateStore()
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
