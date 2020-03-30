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

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

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
public class SourceOperatorTest {
	private static final int NUM_SPLITS = 5;
	private static final int SUBTASK_INDEX = 1;
	private static final MockSourceSplit MOCK_SPLIT = new MockSourceSplit(1234, 10);
	private MockSource source;
	private MockOperatorEventGateway mockGateway;
	private SourceOperator<Integer, MockSourceSplit> operator;

	@Before
	public void setup() {
		this.source = new MockSource(Boundedness.BOUNDED, NUM_SPLITS);
		this.operator = new TestingSourceOperator<>(source, SUBTASK_INDEX);
		this.mockGateway = new MockOperatorEventGateway();
		this.operator.setOperatorEventGateway(mockGateway);
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
		// A source reader should have been created.
		assertEquals(1, source.getCreatedReaders().size());
		MockSourceReader mockSourceReader = source.getCreatedReaders().get(0);
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
		operator.handleOperatorEvent(new AddSplitEvent<>(Collections.singletonList(newSplit)));
		// The source reader should bave been assigned two splits.
		MockSourceReader mockSourceReader = source.getCreatedReaders().get(0);
		assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), mockSourceReader.getAssignedSplits());
	}

	@Test
	public void testHandleAddSourceEvent() throws Exception {
		operator.initializeState(getStateContext());
		operator.open();
		SourceEvent event = new SourceEvent() {};
		operator.handleOperatorEvent(new SourceEventWrapper(event));
		// The source reader should bave been assigned two splits.
		MockSourceReader mockSourceReader = source.getCreatedReaders().get(0);
		assertEquals(Collections.singletonList(event), mockSourceReader.getReceivedSourceEvents());
	}

	@Test
	public void testSnapshotState() throws Exception {
		StateInitializationContext stateContext = getStateContext();
		operator.initializeState(stateContext);
		operator.open();
		MockSourceSplit newSplit = new MockSourceSplit((2));
		operator.handleOperatorEvent(new AddSplitEvent<>(Collections.singletonList(newSplit)));
		operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));

		// Verify the splits in state.
		List<MockSourceSplit> splitsInState = new ArrayList<>();
		Iterable<byte[]> serializedSplits =
				stateContext.getOperatorStateStore().getListState(SourceOperator.SPLITS_STATE_DESC).get();
		for (byte[] serialized : serializedSplits) {
			MockSourceSplitSerializer serializer = new MockSourceSplitSerializer();
			SourceOperator.SplitStateAndVersion stateAndVersion =
					SourceOperator.SplitStateAndVersion.fromBytes(serialized);
			splitsInState.add(serializer.deserialize(
					stateAndVersion.getSerializerVersion(),
					stateAndVersion.getSplitState()));
		}
		assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), splitsInState);
	}

	// ---------------- helper methods -------------------------

	private StateInitializationContext getStateContext() throws Exception {
		// Create a mock split.
		byte[] serializedSplit = new MockSourceSplitSerializer().serialize(MOCK_SPLIT);
		byte[] serializedSplitWithVersion =
				new SourceOperator.SplitStateAndVersion(0, serializedSplit).toBytes();

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

	// -------------- Testing SourceOperator class -------------

	/**
	 * A testing class that overrides the getRuntimeContext() Method.
	 */
	private static class TestingSourceOperator<OUT, SplitT extends SourceSplit> extends
																				SourceOperator<OUT, SplitT> {
		private final int subtaskIndex;

		TestingSourceOperator(Source<OUT, SplitT, ?> source, int subtaskIndex) {
			super(source);
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public StreamingRuntimeContext getRuntimeContext() {
			return new MockStreamingRuntimeContext(false, 5, subtaskIndex);
		}
	}
}
