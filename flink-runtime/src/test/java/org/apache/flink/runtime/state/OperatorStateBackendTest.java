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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.FutureUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OperatorStateBackendTest {

	AbstractStateBackend abstractStateBackend = new MemoryStateBackend(4096);

	static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getUserClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
		return env;
	}

	private DefaultOperatorStateBackend createNewOperatorStateBackend() throws Exception {
		//TODO this is temporarily casted to test already functionality that we do not yet expose through public API
		return (DefaultOperatorStateBackend) abstractStateBackend.createOperatorStateBackend(
				createMockEnvironment(),
				"test-operator");
	}

	@Test
	public void testCreateNew() throws Exception {
		OperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
		assertNotNull(operatorStateBackend);
		assertTrue(operatorStateBackend.getRegisteredStateNames().isEmpty());
	}

	@Test
	public void testRegisterStates() throws Exception {
		DefaultOperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
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
		DefaultOperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
		CheckpointStreamFactory streamFactory =
				abstractStateBackend.createStreamFactory(new JobID(), "testOperator");

		RunnableFuture<OperatorStateHandle> snapshot =
				operatorStateBackend.snapshot(0L, 0L, streamFactory);

		OperatorStateHandle stateHandle = FutureUtil.runIfNotDoneAndGet(snapshot);
		Assert.assertNull(stateHandle);
	}

	@Test
	public void testSnapshotRestore() throws Exception {
		DefaultOperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
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
				operatorStateBackend.snapshot(1, 1, streamFactory));

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

}
