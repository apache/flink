/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend.PartitionableListState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.apache.flink.util.FutureUtil;
import org.junit.Test;

import java.io.Serializable;
import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OperatorStateBackendTest {

	private final ClassLoader classLoader = getClass().getClassLoader();

	@Test
	public void testCreateOnAbstractStateBackend() throws Exception {
		// we use the memory state backend as a subclass of the AbstractStateBackend
		final AbstractStateBackend abstractStateBackend = new MemoryStateBackend();
		OperatorStateBackend operatorStateBackend = abstractStateBackend.createOperatorStateBackend(
				createMockEnvironment(), "test-operator");

		assertNotNull(operatorStateBackend);
		assertTrue(operatorStateBackend.getRegisteredStateNames().isEmpty());
	}

	@Test
	public void testRegisterStatesWithoutTypeSerializer() throws Exception {
		// prepare an execution config with a non standard type registered
		final Class<?> registeredType = FutureTask.class;

		// validate the precondition of this test - if this condition fails, we need to pick a different
		// example serializer
		assertFalse(new KryoSerializer<>(File.class, new ExecutionConfig()).getKryo().getDefaultSerializer(registeredType)
				instanceof com.esotericsoftware.kryo.serializers.JavaSerializer);

		final ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerTypeWithKryoSerializer(registeredType, com.esotericsoftware.kryo.serializers.JavaSerializer.class);

		final DefaultOperatorStateBackend operatorStateBackend = new DefaultOperatorStateBackend(classLoader, cfg);

		ListStateDescriptor<File> stateDescriptor = new ListStateDescriptor<>("test", File.class);
		ListStateDescriptor<String> stateDescriptor2 = new ListStateDescriptor<>("test2", String.class);

		ListState<File> listState = operatorStateBackend.getOperatorState(stateDescriptor);
		assertNotNull(listState);

		ListState<String> listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);
		assertNotNull(listState2);

		assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());

		// make sure that type registrations are forwarded
		TypeSerializer<?> serializer = ((PartitionableListState<?>) listState).getPartitionStateSerializer();
		assertTrue(serializer instanceof KryoSerializer);
		assertTrue(((KryoSerializer<?>) serializer).getKryo().getSerializer(registeredType)
				instanceof com.esotericsoftware.kryo.serializers.JavaSerializer);

		Iterator<String> it = listState2.get().iterator();
		assertFalse(it.hasNext());
		listState2.add("kevin");
		listState2.add("sunny");

		it = listState2.get().iterator();
		assertEquals("kevin", it.next());
		assertEquals("sunny", it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testRegisterStates() throws Exception {
		final DefaultOperatorStateBackend operatorStateBackend =
				new DefaultOperatorStateBackend(classLoader, new ExecutionConfig());

		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor3 = new ListStateDescriptor<>("test3", new JavaSerializer<>());
		ListState<Serializable> listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);
		assertNotNull(listState1);
		assertEquals(1, operatorStateBackend.getRegisteredStateNames().size());
		Iterator<Serializable> it = listState1.get().iterator();
		assertTrue(!it.hasNext());
		listState1.add(42);
		listState1.add(4711);

		it = listState1.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertTrue(!it.hasNext());

		ListState<Serializable> listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);
		assertNotNull(listState2);
		assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());
		assertTrue(!it.hasNext());
		listState2.add(7);
		listState2.add(13);
		listState2.add(23);

		it = listState2.get().iterator();
		assertEquals(7, it.next());
		assertEquals(13, it.next());
		assertEquals(23, it.next());
		assertTrue(!it.hasNext());

		ListState<Serializable> listState3 = operatorStateBackend.getBroadcastOperatorState(stateDescriptor3);
		assertNotNull(listState3);
		assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());
		assertTrue(!it.hasNext());
		listState3.add(17);
		listState3.add(3);
		listState3.add(123);

		it = listState3.get().iterator();
		assertEquals(17, it.next());
		assertEquals(3, it.next());
		assertEquals(123, it.next());
		assertTrue(!it.hasNext());

		ListState<Serializable> listState1b = operatorStateBackend.getOperatorState(stateDescriptor1);
		assertNotNull(listState1b);
		listState1b.add(123);
		it = listState1b.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertTrue(!it.hasNext());

		it = listState1.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertTrue(!it.hasNext());

		it = listState1b.get().iterator();
		assertEquals(42, it.next());
		assertEquals(4711, it.next());
		assertEquals(123, it.next());
		assertTrue(!it.hasNext());

		try {
			operatorStateBackend.getBroadcastOperatorState(stateDescriptor2);
			fail("Did not detect changed mode");
		} catch (IllegalStateException ignored) {

		}

		try {
			operatorStateBackend.getOperatorState(stateDescriptor3);
			fail("Did not detect changed mode");
		} catch (IllegalStateException ignored) {

		}
	}

	@Test
	public void testSnapshotEmpty() throws Exception {
		final AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		final DefaultOperatorStateBackend operatorStateBackend = (DefaultOperatorStateBackend)
				abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "testOperator");

		CheckpointStreamFactory streamFactory =
				abstractStateBackend.createStreamFactory(new JobID(), "testOperator");

		RunnableFuture<OperatorStateHandle> snapshot =
				operatorStateBackend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forFullCheckpoint());

		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(snapshot);
		assertNull(stateHandle);
	}

	@Test
	public void testSnapshotRestore() throws Exception {
		AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

		DefaultOperatorStateBackend operatorStateBackend = (DefaultOperatorStateBackend)
				abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "test-op-name");

		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor3 = new ListStateDescriptor<>("test3", new JavaSerializer<>());
		ListState<Serializable> listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);
		ListState<Serializable> listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);
		ListState<Serializable> listState3 = operatorStateBackend.getBroadcastOperatorState(stateDescriptor3);

		listState1.add(42);
		listState1.add(4711);

		listState2.add(7);
		listState2.add(13);
		listState2.add(23);

		listState3.add(17);
		listState3.add(18);
		listState3.add(19);
		listState3.add(20);

		CheckpointStreamFactory streamFactory = abstractStateBackend.createStreamFactory(new JobID(), "testOperator");
		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(
				operatorStateBackend.snapshot(1, 1, streamFactory, CheckpointOptions.forFullCheckpoint()));

		try {

			operatorStateBackend.close();
			operatorStateBackend.dispose();

			//TODO this is temporarily casted to test already functionality that we do not yet expose through public API
			operatorStateBackend = (DefaultOperatorStateBackend) abstractStateBackend.createOperatorStateBackend(
					createMockEnvironment(),
					"testOperator");

			operatorStateBackend.restore(Collections.singletonList(stateHandle));

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);
			listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);
			listState3 = operatorStateBackend.getBroadcastOperatorState(stateDescriptor3);

			assertEquals(3, operatorStateBackend.getRegisteredStateNames().size());

			Iterator<Serializable> it = listState1.get().iterator();
			assertEquals(42, it.next());
			assertEquals(4711, it.next());
			assertTrue(!it.hasNext());

			it = listState2.get().iterator();
			assertEquals(7, it.next());
			assertEquals(13, it.next());
			assertEquals(23, it.next());
			assertTrue(!it.hasNext());

			it = listState3.get().iterator();
			assertEquals(17, it.next());
			assertEquals(18, it.next());
			assertEquals(19, it.next());
			assertEquals(20, it.next());
			assertTrue(!it.hasNext());

			operatorStateBackend.close();
			operatorStateBackend.dispose();
		} finally {
			stateHandle.discardState();
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(env.getUserClassLoader()).thenReturn(OperatorStateBackendTest.class.getClassLoader());
		return env;
	}
}
