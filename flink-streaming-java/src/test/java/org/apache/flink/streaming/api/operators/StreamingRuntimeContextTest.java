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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.HashKeyGroupAssigner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapListState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamingRuntimeContextTest {
	
	@Test
	public void testValueStateInstantiation() throws Exception {
		
		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);
		
		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();
		
		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createDescriptorCapturingMockOp(descriptorCapture, config),
				createMockEnvironment(),
				Collections.<String, Accumulator<?, ?>>emptyMap());

		ValueStateDescriptor<TaskInfo> descr = new ValueStateDescriptor<>("name", TaskInfo.class, null);
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
				createMockEnvironment(),
				Collections.<String, Accumulator<?, ?>>emptyMap());

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
	public void testListStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createDescriptorCapturingMockOp(descriptorCapture, config),
				createMockEnvironment(),
				Collections.<String, Accumulator<?, ?>>emptyMap());

		ListStateDescriptor<TaskInfo> descr = new ListStateDescriptor<>("name", TaskInfo.class);
		context.getListState(descr);

		StateDescriptor<?, ?> descrIntercepted = (StateDescriptor<?, ?>) descriptorCapture.get();
		TypeSerializer<?> serializer = descrIntercepted.getSerializer();

		// check that the Path class is really registered, i.e., the execution config was applied
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId() > 0);
	}

	@Test
	public void testListStateReturnsEmptyListByDefault() throws Exception {

		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createPlainMockOp(),
				createMockEnvironment(),
				Collections.<String, Accumulator<?, ?>>emptyMap());

		ListStateDescriptor<String> descr = new ListStateDescriptor<>("name", String.class);
		ListState<String> state = context.getListState(descr);

		Iterable<String> value = state.get();
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
		when(operatorMock.getExecutionConfig()).thenReturn(config);
		
		when(operatorMock.getPartitionedState(any(StateDescriptor.class))).thenAnswer(
				new Answer<Object>() {
					
					@Override
					public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
						ref.set(invocationOnMock.getArguments()[0]);
						return null;
					}
				});
		
		return operatorMock;
	}

	@SuppressWarnings("unchecked")
	private static AbstractStreamOperator<?> createPlainMockOp() throws Exception {

		AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);
		when(operatorMock.getExecutionConfig()).thenReturn(new ExecutionConfig());

		when(operatorMock.getPartitionedState(any(ListStateDescriptor.class))).thenAnswer(
				new Answer<ListState<String>>() {

					@Override
					public ListState<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
						ListStateDescriptor<String> descr =
								(ListStateDescriptor<String>) invocationOnMock.getArguments()[0];
						KeyedStateBackend<Integer> backend = new MemoryStateBackend().createKeyedStateBackend(
								new DummyEnvironment("test_task", 1, 0),
								new JobID(),
								"test_op",
								IntSerializer.INSTANCE,
								new HashKeyGroupAssigner<Integer>(1),
								new KeyGroupRange(0, 0),
								new KvStateRegistry().createTaskRegistry(new JobID(),
										new JobVertexID()));
						backend.setCurrentKey(0);
						return backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descr);
					}
				});

		return operatorMock;
	}
	
	private static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getUserClassLoader()).thenReturn(StreamingRuntimeContextTest.class.getClassLoader());
		when(env.getDistributedCacheEntries()).thenReturn(Collections.<String, Future<Path>>emptyMap());
		when(env.getTaskInfo()).thenReturn(new TaskInfo("test task", 1, 0, 1, 1));
		return env;
	}
}
