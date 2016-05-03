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

package org.apache.flink.runtime.state;

import com.google.common.base.Joiner;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.IntValue;

import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Generic tests for the partitioned state part of {@link AbstractStateBackend}.
 */
public abstract class StateBackendTestBase<B extends PartitionedStateBackend<Integer>> extends TestLogger {

	/**
	 * Creates a state backend for the testing base. Every call should create a fresh state backend.
	 *
	 * @return Fresh state backend
	 * @throws Exception
	 */
	protected abstract B createStateBackend() throws Exception;

	protected abstract void setup() throws Exception;

	protected abstract void cleanup() throws Exception;

	@Before
	public void setupTest() throws Exception {
		setup();
	}

	@After
	public void teardown() throws Exception {
		cleanup();
	}

	@Test
	public void testValueState() throws Exception {
		try(B backend = createStateBackend();
			B backend2 = createStateBackend();
			B backend3 = createStateBackend()) {
			ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
			kvId.initializeSerializerUnlessSet(new ExecutionConfig());

			ValueState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			// some modifications to the state
			backend.setCurrentKey(1);
			assertNull(state.value());
			state.update("1");
			backend.setCurrentKey(2);
			assertNull(state.value());
			state.update("2");
			backend.setCurrentKey(1);
			assertEquals("1", state.value());

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key : snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// make some more modifications
			backend.setCurrentKey(1);
			state.update("u1");
			backend.setCurrentKey(2);
			state.update("u2");
			backend.setCurrentKey(3);
			state.update("u3");

			// draw another snapshot
			PartitionedStateSnapshot snapshot2 = backend.snapshotPartitionedState(682375462379L, 4);

			for (String key : snapshot2.keySet()) {
				if (snapshot2.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot2.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot2.get(key)).materialize());
				}
			}

			// validate the original state
			backend.setCurrentKey(1);
			assertEquals("u1", state.value());
			backend.setCurrentKey(2);
			assertEquals("u2", state.value());
			backend.setCurrentKey(3);
			assertEquals("u3", state.value());

			backend2.restorePartitionedState(snapshot1, 100);

			for (String key : snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			ValueState<String> restored1 = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend2.setCurrentKey(1);
			assertEquals("1", restored1.value());
			backend2.setCurrentKey(2);
			assertEquals("2", restored1.value());

			backend3.restorePartitionedState(snapshot2, 100);

			for (String key : snapshot2.keySet()) {
				snapshot2.get(key).discardState();
			}

			ValueState<String> restored2 = backend3.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend3.setCurrentKey(1);
			assertEquals("u1", restored2.value());
			backend3.setCurrentKey(2);
			assertEquals("u2", restored2.value());
			backend3.setCurrentKey(3);
			assertEquals("u3", restored2.value());
		}
	}

	/**
	 * This test verifies that passing {@code null} to {@link ValueState#update(Object)} acts
	 * the same as {@link ValueState#clear()}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testValueStateNullUpdate() throws Exception {

		// precondition: LongSerializer must fail on null value. this way the test would fail
		// later if null values where actually stored in the state instead of acting as clear()
		try {
			LongSerializer.INSTANCE.serialize(null,
				new DataOutputViewStreamWrapper(new ByteArrayOutputStream()));
			fail("Should faill with NullPointerException");
		} catch (NullPointerException e) {
			// alrighty
		}

		try(B backend = createStateBackend();
			B backend2 = createStateBackend()) {
			ValueStateDescriptor<Long> kvId = new ValueStateDescriptor<>("id", LongSerializer.INSTANCE, 42L);
			kvId.initializeSerializerUnlessSet(new ExecutionConfig());

			ValueState<Long> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			// some modifications to the state
			backend.setCurrentKey(1);

			// verify default value
			assertEquals(42L, (long) state.value());
			state.update(1L);
			assertEquals(1L, (long) state.value());

			backend.setCurrentKey(2);
			assertEquals(42L, (long) state.value());

			backend.setCurrentKey(1);
			state.clear();
			assertEquals(42L, (long) state.value());

			state.update(17L);
			assertEquals(17L, (long) state.value());

			state.update(null);
			assertEquals(42L, (long) state.value());

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key : snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			backend2.restorePartitionedState(snapshot1, 100);

			for (String key : snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);
		}
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testListState() throws Exception {
		try(B backend = createStateBackend();
			B backend2 = createStateBackend();
			B backend3 = createStateBackend()) {

			ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);
			ListState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			Joiner joiner = Joiner.on(",");
			// some modifications to the state
			backend.setCurrentKey(1);
			assertEquals("", joiner.join(state.get()));
			state.add("1");
			backend.setCurrentKey(2);
			assertEquals("", joiner.join(state.get()));
			state.add("2");
			backend.setCurrentKey(1);
			assertEquals("1", joiner.join(state.get()));

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// make some more modifications
			backend.setCurrentKey(1);
			state.add("u1");
			backend.setCurrentKey(2);
			state.add("u2");
			backend.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			PartitionedStateSnapshot snapshot2 = backend.snapshotPartitionedState(682375462379L, 4);

			for (String key: snapshot2.keySet()) {
				if (snapshot2.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot2.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot2.get(key)).materialize());
				}
			}

			// validate the original state
			backend.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(state.get()));
			backend.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(state.get()));
			backend.setCurrentKey(3);
			assertEquals("u3", joiner.join(state.get()));

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			ListState<String> restored1 = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend2.setCurrentKey(1);
			assertEquals("1", joiner.join(restored1.get()));
			backend2.setCurrentKey(2);
			assertEquals("2", joiner.join(restored1.get()));

			// restore the second snapshot and validate it
			backend3.restorePartitionedState(snapshot2, 100);

			for (String key: snapshot2.keySet()) {
				snapshot2.get(key).discardState();
			}

			ListState<String> restored2 = backend3.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend3.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(restored2.get()));
			backend3.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(restored2.get()));
			backend3.setCurrentKey(3);
			assertEquals("u3", joiner.join(restored2.get()));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReducingState() {
		try(B backend = createStateBackend();
			B backend2 = createStateBackend();
			B backend3 = createStateBackend()) {
			ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id", new AppendingReduce(), String.class);

			ReducingState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			// some modifications to the state
			backend.setCurrentKey(1);
			assertEquals(null, state.get());
			state.add("1");
			backend.setCurrentKey(2);
			assertEquals(null, state.get());
			state.add("2");
			backend.setCurrentKey(1);
			assertEquals("1", state.get());

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// make some more modifications
			backend.setCurrentKey(1);
			state.add("u1");
			backend.setCurrentKey(2);
			state.add("u2");
			backend.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			PartitionedStateSnapshot snapshot2 = backend.snapshotPartitionedState(682375462379L, 4);

			for (String key: snapshot2.keySet()) {
				if (snapshot2.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot2.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot2.get(key)).materialize());
				}
			}

			// validate the original state
			backend.setCurrentKey(1);
			assertEquals("1,u1", state.get());
			backend.setCurrentKey(2);
			assertEquals("2,u2", state.get());
			backend.setCurrentKey(3);
			assertEquals("u3", state.get());

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			ReducingState<String> restored1 = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend2.setCurrentKey(1);
			assertEquals("1", restored1.get());
			backend2.setCurrentKey(2);
			assertEquals("2", restored1.get());

			// restore the second snapshot and validate it
			backend3.restorePartitionedState(snapshot2, 100);

			for (String key: snapshot2.keySet()) {
				snapshot2.get(key).discardState();
			}

			ReducingState<String> restored2 = backend3.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);


			backend3.setCurrentKey(1);
			assertEquals("1,u1", restored2.get());
			backend3.setCurrentKey(2);
			assertEquals("2,u2", restored2.get());
			backend3.setCurrentKey(3);
			assertEquals("u3", restored2.get());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testFoldingState() {

		try (B backend = createStateBackend();
			 B backend2 = createStateBackend();
			 B backend3 = createStateBackend()) {

			FoldingStateDescriptor<Integer, String> kvId = new FoldingStateDescriptor<>("id",
					"Fold-Initial:",
					new AppendingFold(),
				String.class);

			FoldingState<Integer, String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			// some modifications to the state
			backend.setCurrentKey(1);
			assertEquals("Fold-Initial:", state.get());
			state.add(1);
			backend.setCurrentKey(2);
			assertEquals("Fold-Initial:", state.get());
			state.add(2);
			backend.setCurrentKey(1);
			assertEquals("Fold-Initial:,1", state.get());

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// make some more modifications
			backend.setCurrentKey(1);
			state.clear();
			state.add(101);
			backend.setCurrentKey(2);
			state.add(102);
			backend.setCurrentKey(3);
			state.add(103);

			// draw another snapshot
			PartitionedStateSnapshot snapshot2 = backend.snapshotPartitionedState(682375462379L, 4);

			for (String key: snapshot2.keySet()) {
				if (snapshot2.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot2.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot2.get(key)).materialize());
				}
			}

			// validate the original state
			backend.setCurrentKey(1);
			assertEquals("Fold-Initial:,101", state.get());
			backend.setCurrentKey(2);
			assertEquals("Fold-Initial:,2,102", state.get());
			backend.setCurrentKey(3);
			assertEquals("Fold-Initial:,103", state.get());

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			FoldingState<Integer, String> restored1 = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend2.setCurrentKey(1);
			assertEquals("Fold-Initial:,1", restored1.get());
			backend2.setCurrentKey(2);
			assertEquals("Fold-Initial:,2", restored1.get());

			// restore the second snapshot and validate it
			backend3.restorePartitionedState(snapshot2, 100);

			for (String key: snapshot2.keySet()) {
				snapshot2.get(key).discardState();
			}

			@SuppressWarnings("unchecked")
			FoldingState<Integer, String> restored2 = backend3.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend3.setCurrentKey(1);
			assertEquals("Fold-Initial:,101", restored2.get());
			backend3.setCurrentKey(2);
			assertEquals("Fold-Initial:,2,102", restored2.get());
			backend3.setCurrentKey(3);
			assertEquals("Fold-Initial:,103", restored2.get());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testValueStateRestoreWithWrongSerializers() {
		try (B backend = createStateBackend();
			 B backend2 = createStateBackend()){

			ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
			kvId.initializeSerializerUnlessSet(new ExecutionConfig());
			
			ValueState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.update("1");
			backend.setCurrentKey(2);
			state.update("2");

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
				(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ValueStateDescriptor<>("id", fakeStringSerializer, null);

				state = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

				state.value();

				fail("should recognize wrong serializers");
			} catch (RuntimeException e) {
				if (!e.getMessage().contains("Trying to access state using wrong StateDescriptor")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testListStateRestoreWithWrongSerializers() {
		try (B backend = createStateBackend();
			 B backend2 = createStateBackend()){

			ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", String.class);
			ListState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.add("1");
			backend.setCurrentKey(2);
			state.add("2");

			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ListStateDescriptor<>("id", fakeStringSerializer);

				state = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

				state.get();

				fail("should recognize wrong serializers");
			} catch (RuntimeException e) {
				if (!e.getMessage().contains("Trying to access state using wrong StateDescriptor")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReducingStateRestoreWithWrongSerializers() {
		try (B backend = createStateBackend();
			 B backend2 = createStateBackend()) {

			ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id",
					new AppendingReduce(),
					StringSerializer.INSTANCE);
			ReducingState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.add("1");
			backend.setCurrentKey(2);
			state.add("2");


			// draw a snapshot
			PartitionedStateSnapshot snapshot1 = backend.snapshotPartitionedState(682375462378L, 2);

			for (String key: snapshot1.keySet()) {
				if (snapshot1.get(key) instanceof AsynchronousKvStateSnapshot) {
					snapshot1.put(key, ((AsynchronousKvStateSnapshot<?, ?, ?>) snapshot1.get(key)).materialize());
				}
			}

			// restore the first snapshot and validate it
			backend2.restorePartitionedState(snapshot1, 100);

			for (String key: snapshot1.keySet()) {
				snapshot1.get(key).discardState();
			}

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				kvId = new ReducingStateDescriptor<>("id", new AppendingReduce(), fakeStringSerializer);

				state = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

				state.get();

				fail("should recognize wrong serializers");
			} catch (RuntimeException e) {
				if (!e.getMessage().contains("Trying to access state using wrong StateDescriptor")) {
					fail("wrong exception " + e);
				}
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCopyDefaultValue() throws Exception {
		try (B backend = createStateBackend()) {

			ValueStateDescriptor<IntValue> kvId = new ValueStateDescriptor<>("id", IntValue.class, new IntValue(-1));
			kvId.initializeSerializerUnlessSet(new ExecutionConfig());

			ValueState<IntValue> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			IntValue default1 = state.value();

			backend.setCurrentKey(2);
			IntValue default2 = state.value();

			assertNotNull(default1);
			assertNotNull(default2);
			assertEquals(default1, default2);
			assertFalse(default1 == default2);
		}
	}

	private static class AppendingReduce implements ReduceFunction<String> {
		private static final long serialVersionUID = 6860242820044380228L;

		@Override
		public String reduce(String value1, String value2) throws Exception {
			return value1 + "," + value2;
		}
	}

	private static class AppendingFold implements FoldFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String acc, Integer value) throws Exception {
			return acc + "," + value;
		}
	}
}
