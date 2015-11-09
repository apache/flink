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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateIdentifier;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.IntValueSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.types.IntValue;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * Tests for the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend}.
 */
public class MemoryStateBackendTest {

	@Test
	public void testSerializableState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			StateHandle<HashMap<String, Integer>> handle = backend.checkpointStateSerializable(state, 12, 459);
			assertNotNull(handle);

			HashMap<String, Integer> restored = handle.getState(getClass().getClassLoader());
			assertEquals(state, restored);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOversizedState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			try {
				backend.checkpointStateSerializable(state, 12, 459);
				fail("this should cause an exception");
			}
			catch (IOException e) {
				// now darling, isn't that exactly what we wanted?
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			AbstractStateBackend.CheckpointStateOutputStream os = backend.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(state);
			oos.flush();
			StreamStateHandle handle = os.closeAndGetHandle();

			assertNotNull(handle);

			ObjectInputStream ois = new ObjectInputStream(handle.getState(getClass().getClassLoader()));
			assertEquals(state, ois.readObject());
			assertTrue(ois.available() <= 0);
			ois.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOversizedStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			AbstractStateBackend.CheckpointStateOutputStream os = backend.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);

			try {
				oos.writeObject(state);
				oos.flush();
				os.closeAndGetHandle();
				fail("this should cause an exception");
			}
			catch (IOException e) {
				// oh boy! what an exception!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testValueState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ValueStateIdentifier<String> kvId = new ValueStateIdentifier<>("id", null, StringSerializer.INSTANCE);
			ValueState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> kv =
					(KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend>) state;

			assertEquals(0, kv.size());

			// some modifications to the state
			kv.setCurrentKey(1);
			assertNull(state.value());
			state.update("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertNull(state.value());
			state.update("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", state.value());
			assertEquals(2, kv.size());

			// draw a snapshot
			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> snapshot1 =
					kv.snapshot(682375462378L, System.currentTimeMillis());

			// make some more modifications
			kv.setCurrentKey(1);
			state.update("u1");
			kv.setCurrentKey(2);
			state.update("u2");
			kv.setCurrentKey(3);
			state.update("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("u1", state.value());
			kv.setCurrentKey(2);
			assertEquals("u2", state.value());
			kv.setCurrentKey(3);
			assertEquals("u3", state.value());

			// restore the first snapshot and validate it
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> restored1 = snapshot1.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ValueState<String> restored1State = (ValueState<String>) restored1;

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", restored1State.value());
			restored1.setCurrentKey(2);
			assertEquals("2", restored1State.value());

			// restore the second snapshot and validate it
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> restored2 = snapshot2.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ValueState<String> restored2State = (ValueState<String>) restored2;

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("u1", restored2State.value());
			restored2.setCurrentKey(2);
			assertEquals("u2", restored2State.value());
			restored2.setCurrentKey(3);
			assertEquals("u3", restored2State.value());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testListState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ListStateIdentifier<String> kvId = new ListStateIdentifier<>("id", StringSerializer.INSTANCE);
			ListState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> kv =
					(KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend>) state;

			assertEquals(0, kv.size());

			Joiner joiner = Joiner.on(",");
			// some modifications to the state
			kv.setCurrentKey(1);
			assertEquals("", joiner.join(state));
			state.add("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertEquals("", joiner.join(state));
			state.add("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", joiner.join(state));
			assertEquals(2, kv.size());

			// draw a snapshot
			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> snapshot1 =
					kv.snapshot(682375462378L, System.currentTimeMillis());

			// make some more modifications
			kv.setCurrentKey(1);
			state.add("u1");
			kv.setCurrentKey(2);
			state.add("u2");
			kv.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(state));
			kv.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(state));
			kv.setCurrentKey(3);
			assertEquals("u3", joiner.join(state));

			// restore the first snapshot and validate it
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> restored1 = snapshot1.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ListState<String> restored1State = (ListState<String>) restored1;

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", joiner.join(restored1State));
			restored1.setCurrentKey(2);
			assertEquals("2", joiner.join(restored1State));

			// restore the second snapshot and validate it
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> restored2 = snapshot2.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ListState<String> restored2State = (ListState<String>) restored2;

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(restored2State));
			restored2.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(restored2State));
			restored2.setCurrentKey(3);
			assertEquals("u3", joiner.join(restored2State));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void testValueStateRestoreWithWrongSerializers() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ValueStateIdentifier<String> kvId = new ValueStateIdentifier<>("id", null, StringSerializer.INSTANCE);
			ValueState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> kv =
					(KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend>) state;

			kv.setCurrentKey(1);
			state.update("1");
			kv.setCurrentKey(2);
			state.update("2");

			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, MemoryStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());



			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						kvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}


			@SuppressWarnings("unchecked")
			ValueStateIdentifier<String> fakeKvId =
					(ValueStateIdentifier<String>)(ValueStateIdentifier<?>) new ValueStateIdentifier<>("id", null, FloatSerializer.INSTANCE);

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testListStateRestoreWithWrongSerializers() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ListStateIdentifier<String> kvId = new ListStateIdentifier<>("id", StringSerializer.INSTANCE);
			ListState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> kv =
					(KvState<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend>) state;

			kv.setCurrentKey(1);
			state.add("1");
			kv.setCurrentKey(2);
			state.add("2");

			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, MemoryStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());



			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						kvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}


			@SuppressWarnings("unchecked")
			ListStateIdentifier<String> fakeKvId =
					(ListStateIdentifier<String>)(ListStateIdentifier<?>) new ListStateIdentifier<>("id", FloatSerializer.INSTANCE);

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void testCopyDefaultValue() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ValueStateIdentifier<IntValue> kvId = new ValueStateIdentifier<>("id", new IntValue(-1), IntValueSerializer.INSTANCE);
			ValueState<IntValue> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<IntValue>, ValueStateIdentifier<IntValue>, MemoryStateBackend> kv =
					(KvState<Integer, ValueState<IntValue>, ValueStateIdentifier<IntValue>, MemoryStateBackend>) state;

			kv.setCurrentKey(1);
			IntValue default1 = state.value();

			kv.setCurrentKey(2);
			IntValue default2 = state.value();

			assertNotNull(default1);
			assertNotNull(default2);
			assertEquals(default1, default2);
			assertFalse(default1 == default2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
