/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the simple Java heap objects implementation of the {@link ReducingState}.
 */
public class HeapReducingStateTest extends HeapStateBackendTestBase {

	@Test
	public void testAddAndGet() throws Exception {

		final ReducingStateDescriptor<Long> stateDescr =
				new ReducingStateDescriptor<>("my-state", new AddingFunction(), Long.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend();

		try {
			InternalReducingState<VoidNamespace, Long> state =
					keyedBackend.createReducingState(VoidNamespaceSerializer.INSTANCE, stateDescr);
			state.setCurrentNamespace(VoidNamespace.INSTANCE);

			keyedBackend.setCurrentKey("abc");
			assertNull(state.get());

			keyedBackend.setCurrentKey("def");
			assertNull(state.get());
			state.add(17L);
			state.add(11L);
			assertEquals(28L, state.get().longValue());

			keyedBackend.setCurrentKey("abc");
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			assertNull(state.get());
			state.add(1L);
			state.add(2L);

			keyedBackend.setCurrentKey("def");
			assertEquals(28L, state.get().longValue());
			state.clear();
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			state.add(3L);
			state.add(2L);
			state.add(1L);

			keyedBackend.setCurrentKey("def");
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			assertEquals(9L, state.get().longValue());
			state.clear();

			// make sure all lists / maps are cleared

			StateTable<String, VoidNamespace, Long> stateTable =
					((HeapReducingState<String, VoidNamespace, Long>) state).stateTable;

			assertTrue(stateTable.isEmpty());
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

	@Test
	public void testMerging() throws Exception {

		final ReducingStateDescriptor<Long> stateDescr = new ReducingStateDescriptor<>(
				"my-state", new AddingFunction(), Long.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		final Long expectedResult = 165L;

		final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend();

		try {
			final InternalReducingState<Integer, Long> state =
					keyedBackend.createReducingState(IntSerializer.INSTANCE, stateDescr);

			// populate the different namespaces
			//  - abc spreads the values over three namespaces
			//  - def spreads teh values over two namespaces (one empty)
			//  - ghi is empty
			//  - jkl has all elements already in the target namespace
			//  - mno has all elements already in one source namespace

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			state.add(33L);
			state.add(55L);

			state.setCurrentNamespace(namespace2);
			state.add(22L);
			state.add(11L);

			state.setCurrentNamespace(namespace3);
			state.add(44L);

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			state.add(11L);
			state.add(44L);

			state.setCurrentNamespace(namespace3);
			state.add(22L);
			state.add(55L);
			state.add(33L);

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace3);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("abc");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("def");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("ghi");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertNull(state.get());

			keyedBackend.setCurrentKey("jkl");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("mno");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertEquals(expectedResult, state.get());

			// make sure all lists / maps are cleared

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("ghi");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace1);
			state.clear();

			StateTable<String, Integer, Long> stateTable =
					((HeapReducingState<String, Integer, Long>) state).stateTable;

			assertTrue(stateTable.isEmpty());
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

	// ------------------------------------------------------------------------
	//  test functions
	// ------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static class AddingFunction implements ReduceFunction<Long> {

		@Override
		public Long reduce(Long a, Long b)  {
			return a + b;
		}
	}
}
