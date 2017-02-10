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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests for the {@link InternalAggregatingState} implementation on top of RocksDB.
 */
public class RocksDBAggregatingStateTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------

	@Test
	public void testAddAndGet() throws Exception {

		final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
				new AggregatingStateDescriptor<>("my-state", new AddingFunction(), MutableLong.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final RocksDBStateBackend backend = new RocksDBStateBackend(tmp.newFolder().toURI());
		backend.setDbStoragePath(tmp.newFolder().getAbsolutePath());

		final RocksDBKeyedStateBackend<String> keyedBackend = createKeyedBackend(backend);

		try {
			InternalAggregatingState<VoidNamespace, Long, Long> state =
					keyedBackend.createAggregatingState(VoidNamespaceSerializer.INSTANCE, stateDescr);
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
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

	@Test
	public void testMerging() throws Exception {

		final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
				new AggregatingStateDescriptor<>("my-state", new AddingFunction(), MutableLong.class);
		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

		final TimeWindow win1 = new TimeWindow(1000, 2000);
		final TimeWindow win2 = new TimeWindow(2000, 3000);
		final TimeWindow win3 = new TimeWindow(3000, 4000);

		final Long expectedResult = 165L;

		final RocksDBStateBackend backend = new RocksDBStateBackend(tmp.newFolder().toURI());
		backend.setDbStoragePath(tmp.newFolder().getAbsolutePath());

		final RocksDBKeyedStateBackend<String> keyedBackend = createKeyedBackend(backend);

		try {
			InternalAggregatingState<TimeWindow, Long, Long> state =
					keyedBackend.createAggregatingState(new TimeWindow.Serializer(), stateDescr);

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
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("def");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("ghi");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			assertNull(state.get());

			keyedBackend.setCurrentKey("jkl");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			assertEquals(expectedResult, state.get());

			keyedBackend.setCurrentKey("mno");
			state.mergeNamespaces(win1, asList(win2, win3));
			state.setCurrentNamespace(win1);
			assertEquals(expectedResult, state.get());
		}
		finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}

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

	//  test functions
	// ------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static class AddingFunction implements AggregateFunction<Long, MutableLong, Long> {

		@Override
		public MutableLong createAccumulator() {
			return new MutableLong();
		}

		@Override
		public void add(Long value, MutableLong accumulator) {
			accumulator.value += value;
		}

		@Override
		public Long getResult(MutableLong accumulator) {
			return accumulator.value;
		}

		@Override
		public MutableLong merge(MutableLong a, MutableLong b) {
			a.value += b.value;
			return a;
		}
	}

	private static final class MutableLong {
		long value;
	}
}
