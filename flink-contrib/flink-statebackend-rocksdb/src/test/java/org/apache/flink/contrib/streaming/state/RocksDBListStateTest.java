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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link ListState} implementation on top of RocksDB.
 */
public class RocksDBListStateTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------

	@Test
	public void testAddAndGet() throws Exception {

		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final RocksDBStateBackend backend = new RocksDBStateBackend(tmp.newFolder().toURI());
		backend.setDbStoragePath(tmp.newFolder().getAbsolutePath());

		final RocksDBKeyedStateBackend<String> keyedBackend = createKeyedBackend(backend);
		
		try {
			InternalListState<VoidNamespace, Long> state = 
					keyedBackend.createListState(VoidNamespaceSerializer.INSTANCE, stateDescr);
			state.setCurrentNamespace(VoidNamespace.INSTANCE);

			keyedBackend.setCurrentKey("abc");
			assertNull(state.get());

			keyedBackend.setCurrentKey("def");
			assertNull(state.get());
			state.add(17L);
			state.add(11L);
			assertEquals(asList(17L, 11L), state.get());

			keyedBackend.setCurrentKey("abc");
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			assertNull(state.get());
			state.add(1L);
			state.add(2L);

			keyedBackend.setCurrentKey("def");
			assertEquals(asList(17L, 11L), state.get());
			state.clear();
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			state.add(3L);
			state.add(2L);
			state.add(1L);

			keyedBackend.setCurrentKey("def");
			assertNull(state.get());

			keyedBackend.setCurrentKey("g");
			assertEquals(asList(1L, 2L, 3L, 2L, 1L), state.get());
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

	@Test
	public void testMerging() throws Exception {

		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final TimeWindow win1 = new TimeWindow(1000, 2000);
		final TimeWindow win2 = new TimeWindow(2000, 3000);
		final TimeWindow win3 = new TimeWindow(3000, 4000);

		final Set<Long> expectedResult = new HashSet<>(asList(11L, 22L, 33L, 44L, 55L));

		final RocksDBStateBackend backend = new RocksDBStateBackend(tmp.newFolder().toURI());
		backend.setDbStoragePath(tmp.newFolder().getAbsolutePath());

		final RocksDBKeyedStateBackend<String> keyedBackend = createKeyedBackend(backend);

		try {
			InternalListState<TimeWindow, Long> state = keyedBackend.createListState(new TimeWindow.Serializer(), stateDescr);

			// populate the different namespaces
			//  - abc spreads the values over three namespaces
			//  - def spreads teh values over two namespaces (one empty)
			//  - ghi is empty
			//  - jkl has all elements already in the target namespace
			//  - mno has all elements already in one source namespace

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(win1);
			state.add(33L);
			state.add(55L);

			state.setCurrentNamespace(win2);
			state.add(22L);
			state.add(11L);

			state.setCurrentNamespace(win3);
			state.add(44L);

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(win1);
			state.add(11L);
			state.add(44L);

			state.setCurrentNamespace(win3);
			state.add(22L);
			state.add(55L);
			state.add(33L);

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(win1);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(win3);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("abc");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			validateResult(state.get(), expectedResult);

			keyedBackend.setCurrentKey("def");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			validateResult(state.get(), expectedResult);

			keyedBackend.setCurrentKey("ghi");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			assertNull(state.get());

			keyedBackend.setCurrentKey("jkl");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			validateResult(state.get(), expectedResult);

			keyedBackend.setCurrentKey("mno");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			validateResult(state.get(), expectedResult);
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static RocksDBKeyedStateBackend<String> createKeyedBackend(RocksDBStateBackend backend) throws Exception {
		return (RocksDBKeyedStateBackend<String>) backend.createKeyedStateBackend(
				new DummyEnvironment("TestTask", 1, 0),
				new JobID(),
				"test-op",
				StringSerializer.INSTANCE,
				16,
				new KeyGroupRange(2, 3),
				mock(TaskKvStateRegistry.class));
	}

	private static <T> void validateResult(Iterable<T> values, Set<T> expected) {
		int num = 0;
		for (T v : values) {
			num++;
			assertTrue(expected.contains(v));
		}

		assertEquals(expected.size(), num);
	}
}
