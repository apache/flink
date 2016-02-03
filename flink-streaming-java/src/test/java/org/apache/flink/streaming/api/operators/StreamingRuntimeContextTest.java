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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;

import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class StreamingRuntimeContextTest {
	
	@Test
	public void testValueStateInstantiation() throws Exception {
		
		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);
		
		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();
		
		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createMockOp(descriptorCapture, config),
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
	public void testReduceingStateInstantiation() throws Exception {

		final ExecutionConfig config = new ExecutionConfig();
		config.registerKryoType(Path.class);

		final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

		StreamingRuntimeContext context = new StreamingRuntimeContext(
				createMockOp(descriptorCapture, config),
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
				createMockOp(descriptorCapture, config),
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
	
	// ------------------------------------------------------------------------
	//  
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	private static AbstractStreamOperator<?> createMockOp(
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
	
	private static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getUserClassLoader()).thenReturn(StreamingRuntimeContextTest.class.getClassLoader());
		when(env.getDistributedCacheEntries()).thenReturn(Collections.<String, Future<Path>>emptyMap());
		when(env.getTaskInfo()).thenReturn(new TaskInfo("test task", 0, 1, 1));
		return env;
	}
}
