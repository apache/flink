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
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OperatorStateBackendTest {

	AbstractStateBackend abstractStateBackend = new MemoryStateBackend(1024);

	static Environment createMockEnvironment() {
		Environment env = mock(Environment.class);
		when(env.getUserClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
		return env;
	}

	private OperatorStateBackend createNewOperatorStateBackend() throws Exception {
		return abstractStateBackend.createOperatorStateBackend(createMockEnvironment(), "test-operator");
	}

	@Test
	public void testCreateNew() throws Exception {
		OperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
		assertNotNull(operatorStateBackend);
		assertTrue(operatorStateBackend.getRegisteredStateNames().isEmpty());
	}

	@Test
	public void testRegisterStates() throws Exception {
		OperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
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
	}

	@Test
	public void testSnapshotRestore() throws Exception {
		OperatorStateBackend operatorStateBackend = createNewOperatorStateBackend();
		ListStateDescriptor<Serializable> stateDescriptor1 = new ListStateDescriptor<>("test1", new JavaSerializer<>());
		ListStateDescriptor<Serializable> stateDescriptor2 = new ListStateDescriptor<>("test2", new JavaSerializer<>());
		ListState<Serializable> listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);
		ListState<Serializable> listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);

		listState1.add(42);
		listState1.add(4711);

		listState2.add(7);
		listState2.add(13);
		listState2.add(23);

		CheckpointStreamFactory streamFactory = abstractStateBackend.createStreamFactory(new JobID(), "testOperator");
		OperatorStateHandle stateHandle = operatorStateBackend.snapshot(1, 1, streamFactory).get();

		try {

			operatorStateBackend.dispose();

			operatorStateBackend = abstractStateBackend.restoreOperatorStateBackend(
					createMockEnvironment(), "testOperator", Collections.singletonList(stateHandle));

			assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());

			listState1 = operatorStateBackend.getOperatorState(stateDescriptor1);
			listState2 = operatorStateBackend.getOperatorState(stateDescriptor2);

			assertEquals(2, operatorStateBackend.getRegisteredStateNames().size());


			Iterator<Serializable> it = listState1.get().iterator();
			assertEquals(42, it.next());
			assertEquals(4711, it.next());
			assertTrue(!it.hasNext());

			it = listState2.get().iterator();
			assertEquals(7, it.next());
			assertEquals(13, it.next());
			assertEquals(23, it.next());
			assertTrue(!it.hasNext());

			operatorStateBackend.dispose();
		} finally {
			stateHandle.discardState();
		}
	}

}
