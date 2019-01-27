/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.context.ContextStateHelper;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamingRuntimeContext}.
 */
public class StreamingRuntimeContextTest {

	@Test
	public void testValueStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		ValueStateDescriptor<TaskInfo> descr = new ValueStateDescriptor<>("name", TaskInfo.class);
		context.getState(descr);

		StateDescriptor<?, ?> descrIntercepted = (StateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testReducingStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		@SuppressWarnings("unchecked")
		ReduceFunction<TaskInfo> reducer = (ReduceFunction<TaskInfo>) mock(ReduceFunction.class);

		ReducingStateDescriptor<TaskInfo> descr =
			new ReducingStateDescriptor<>("name", reducer, TaskInfo.class);

		context.getReducingState(descr);

		StateDescriptor<?, ?> descrIntercepted = (StateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testAggregatingStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		@SuppressWarnings("unchecked")
		AggregateFunction<String, TaskInfo, String> aggregate = (AggregateFunction<String, TaskInfo, String>) mock(AggregateFunction.class);

		AggregatingStateDescriptor<String, TaskInfo, String> descr =
			new AggregatingStateDescriptor<>("name", aggregate, TaskInfo.class);

		context.getAggregatingState(descr);

		AggregatingStateDescriptor<?, ?, ?> descrIntercepted = (AggregatingStateDescriptor<?, ?, ?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testFoldingStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		@SuppressWarnings("unchecked")
		FoldFunction<String, TaskInfo> folder = (FoldFunction<String, TaskInfo>) mock(FoldFunction.class);

		FoldingStateDescriptor<String, TaskInfo> descr =
			new FoldingStateDescriptor<>("name", null, folder, TaskInfo.class);

		context.getFoldingState(descr);

		FoldingStateDescriptor<?, ?> descrIntercepted = (FoldingStateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testListStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		ListStateDescriptor<TaskInfo> descr = new ListStateDescriptor<>("name", TaskInfo.class);
		context.getListState(descr);

		ListStateDescriptor<?> descrIntercepted = (ListStateDescriptor<?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof ListSerializer);

		TypeSerializer<?> elementSerializer = descrIntercepted.getElementSerializer();
		assertTrue(elementSerializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) elementSerializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testListStateReturnsEmptyListByDefault() throws Exception {

		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createListPlainMockOp(),
				createMockEnvironment());

		ListStateDescriptor<String> descr = new ListStateDescriptor<>("name", String.class);
		ListState<String> state = context.getListState(descr);

		Iterable<String> value = state.get();
		assertNotNull(value);
		assertFalse(value.iterator().hasNext());
	}

	@Test
	public void testMapStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		MapStateDescriptor<String, TaskInfo> descr =
			new MapStateDescriptor<>("name", String.class, TaskInfo.class);

		context.getMapState(descr);

		MapStateDescriptor<?, ?> descrIntercepted = (MapStateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> valueSerializer = descrIntercepted.getValueSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(valueSerializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) valueSerializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testMapStateReturnsEmptyMapByDefault() throws Exception {

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createMapPlainMockOp(),
			createMockEnvironment());

		MapStateDescriptor<Integer, String> descr = new MapStateDescriptor<>("name", Integer.class, String.class);
		MapState<Integer, String> state = context.getMapState(descr);

		Iterable<Map.Entry<Integer, String>> value = state.entries();
		assertNotNull(value);
		assertFalse(value.iterator().hasNext());
	}

	@Test
	public void testSortedMapStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createDescriptorCapturingMockOp(descriptorCapture, config),
			createMockEnvironment());

		SortedMapStateDescriptor<Long, TaskInfo> descr =
			new SortedMapStateDescriptor<>("name", BytewiseComparator.LONG_INSTANCE, Long.class, TaskInfo.class);

		context.getSortedMapState(descr);

		SortedMapStateDescriptor<?, ?> descrIntercepted = (SortedMapStateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> valueSerializer = descrIntercepted.getValueSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(valueSerializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) valueSerializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testSortedMapStateReturnsEmptyMapByDefault() throws Exception {

		StreamingRuntimeContext context = new StreamingRuntimeContext(
			createMapPlainMockOp(),
			createMockEnvironment());

		SortedMapStateDescriptor<Integer, String> descr =
			new SortedMapStateDescriptor<>("name", BytewiseComparator.INT_INSTANCE, Integer.class, String.class);
		MapState<Integer, String> state = context.getSortedMapState(descr);

		Iterable<Map.Entry<Integer, String>> value = state.entries();
		assertNotNull(value);
		assertFalse(value.iterator().hasNext());
	}

	// ------------------------------------------------------------------------
	//
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static AbstractStreamOperator<?> createDescriptorCapturingMockOp(
		final AtomicReference<Object> ref, final ExecutionConfig config) throws Exception {

		AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);

		AbstractInternalStateBackend internalStateBackend = new MemoryStateBackend().createInternalStateBackend(
			new DummyEnvironment("test_task", 1, 0),
			"test_op",
			1,
			new KeyGroupRange(0, 0));
		KeyContextImpl keyContext = new KeyContextImpl(IntSerializer.INSTANCE, 1, new KeyGroupRange(0, 0));

		ContextStateHelper contextStateHelper =
			new ContextStateHelper(keyContext, config, internalStateBackend);

		MockKeyedStateStore keyedStateStore = spy(new MockKeyedStateStore(contextStateHelper, config));

		when(operatorMock.getExecutionConfig()).thenReturn(config);

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				ref.set(invocationOnMock.getArguments()[0]);
				return null;
			}
		}).when(keyedStateStore).getPartitionedState(any(StateDescriptor.class));

		when(operatorMock.getKeyedStateStore()).thenReturn(keyedStateStore);
		when(operatorMock.getOperatorID()).thenReturn(new OperatorID());

		return operatorMock;
	}

	@SuppressWarnings("unchecked")
	private static AbstractStreamOperator<?> createListPlainMockOp() throws Exception {

		AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);
		ExecutionConfig config = new ExecutionConfig();

		when(operatorMock.getExecutionConfig()).thenReturn(config);

		AbstractInternalStateBackend internalStateBackend = new MemoryStateBackend().createInternalStateBackend(
			new DummyEnvironment("test_task", 1, 0),
			"test_op",
			1,
			new KeyGroupRange(0, 0));
		KeyContextImpl keyContext = new KeyContextImpl(IntSerializer.INSTANCE, 1, new KeyGroupRange(0, 0));

		ContextStateHelper contextStateHelper =
			new ContextStateHelper(keyContext, config, internalStateBackend);

		MockKeyedStateStore keyedStateStore = spy(new MockKeyedStateStore(contextStateHelper, config));

		when(operatorMock.getExecutionConfig()).thenReturn(config);

		doAnswer(new Answer<ListState<String>>() {

			@Override
			public ListState<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
				ListStateDescriptor<String> descr =
					(ListStateDescriptor<String>) invocationOnMock.getArguments()[0];

				keyContext.setCurrentKey(0);
				return contextStateHelper.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descr);
			}
		}).when(keyedStateStore).getPartitionedState(any(ListStateDescriptor.class));

		when(operatorMock.getKeyedStateStore()).thenReturn(keyedStateStore);
		when(operatorMock.getOperatorID()).thenReturn(new OperatorID());
		return operatorMock;
	}

	@SuppressWarnings("unchecked")
	private static AbstractStreamOperator<?> createMapPlainMockOp() throws Exception {

		AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);
		ExecutionConfig config = new ExecutionConfig();

		AbstractInternalStateBackend internalStateBackend = new MemoryStateBackend().createInternalStateBackend(
			new DummyEnvironment("test_task", 1, 0),
			"test_op",
			1,
			new KeyGroupRange(0, 0));
		KeyContextImpl keyContext = new KeyContextImpl(IntSerializer.INSTANCE, 1, new KeyGroupRange(0, 0));

		ContextStateHelper contextStateHelper =
			new ContextStateHelper(keyContext, config, internalStateBackend);

		MockKeyedStateStore keyedStateStore = spy(new MockKeyedStateStore(contextStateHelper, config));

		when(operatorMock.getExecutionConfig()).thenReturn(config);

		doAnswer(new Answer<MapState<Integer, String>>() {

			@Override
			public MapState<Integer, String> answer(InvocationOnMock invocationOnMock) throws Throwable {
				MapStateDescriptor<Integer, String> descr =
					(MapStateDescriptor<Integer, String>) invocationOnMock.getArguments()[0];
				keyContext.setCurrentKey(0);
				return contextStateHelper.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descr);
			}
		}).when(keyedStateStore).getPartitionedState(any(MapStateDescriptor.class));

		doAnswer(new Answer<SortedMapState<Integer, String>>() {

			@Override
			public SortedMapState<Integer, String> answer(InvocationOnMock invocationOnMock) throws Throwable {
				SortedMapStateDescriptor<Integer, String> descr =
					(SortedMapStateDescriptor<Integer, String>) invocationOnMock.getArguments()[0];
				keyContext.setCurrentKey(0);
				return contextStateHelper.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descr);
			}
		}).when(keyedStateStore).getSortedMapState(any(SortedMapStateDescriptor.class));

		when(operatorMock.getKeyedStateStore()).thenReturn(keyedStateStore);
		when(operatorMock.getOperatorID()).thenReturn(new OperatorID());
		return operatorMock;
	}

	private static Environment createMockEnvironment() {
		return MockEnvironment.builder()
			.setTaskName("test task")
			.build();
	}

	private static class MockKeyedStateStore extends DefaultKeyedStateStore {

		public MockKeyedStateStore(StateBinder contextStateBinder, ExecutionConfig executionConfig) {
			super(contextStateBinder, executionConfig);
		}

		@Override
		protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return super.getPartitionedState(stateDescriptor);
		}
	}
}
