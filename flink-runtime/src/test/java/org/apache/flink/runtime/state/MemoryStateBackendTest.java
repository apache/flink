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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.IntValueSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
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

			StateBackend.CheckpointStateOutputStream os = backend.createCheckpointStateOutputStream(1, 2);
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

			StateBackend.CheckpointStateOutputStream os = backend.createCheckpointStateOutputStream(1, 2);
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
	public void testKeyValueState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();

			KvState<Integer, String, MemoryStateBackend> kv =
					backend.createKvState(0, "s", IntSerializer.INSTANCE, StringSerializer.INSTANCE, null);

			assertEquals(0, kv.size());

			// some modifications to the state
			kv.setCurrentKey(1);
			assertNull(kv.value());
			kv.update("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertNull(kv.value());
			kv.update("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", kv.value());
			assertEquals(2, kv.size());

			// draw a snapshot
			KvStateSnapshot<Integer, String, MemoryStateBackend> snapshot1 = 
					kv.snapshot(682375462378L, System.currentTimeMillis());
			
			// make some more modifications
			kv.setCurrentKey(1);
			kv.update("u1");
			kv.setCurrentKey(2);
			kv.update("u2");
			kv.setCurrentKey(3);
			kv.update("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, String, MemoryStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());
			
			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("u1", kv.value());
			kv.setCurrentKey(2);
			assertEquals("u2", kv.value());
			kv.setCurrentKey(3);
			assertEquals("u3", kv.value());

			// restore the first snapshot and validate it
			KvState<Integer, String, MemoryStateBackend> restored1 = snapshot1.restoreState(backend,
							IntSerializer.INSTANCE, StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", restored1.value());
			restored1.setCurrentKey(2);
			assertEquals("2", restored1.value());

			// restore the first snapshot and validate it
			KvState<Integer, String, MemoryStateBackend> restored2 = snapshot2.restoreState(backend,
					IntSerializer.INSTANCE, StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("u1", restored2.value());
			restored2.setCurrentKey(2);
			assertEquals("u2", restored2.value());
			restored2.setCurrentKey(3);
			assertEquals("u3", restored2.value());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRestoreWithWrongSerializers() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			KvState<Integer, String, MemoryStateBackend> kv =
					backend.createKvState(0, "s", IntSerializer.INSTANCE, StringSerializer.INSTANCE, null);

			kv.setCurrentKey(1);
			kv.update("1");
			kv.setCurrentKey(2);
			kv.update("2");

			KvStateSnapshot<Integer, String, MemoryStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());


			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) new ValueSerializer<StringValue>(StringValue.class);

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeStringSerializer, null, getClass().getClassLoader(), 1);
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeStringSerializer, null, getClass().getClassLoader(), 1);
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
			KvState<Integer, IntValue, MemoryStateBackend> kv =
					backend.createKvState(0, "a", IntSerializer.INSTANCE, IntValueSerializer.INSTANCE, new IntValue(-1));

			kv.setCurrentKey(1);
			IntValue default1 = kv.value();

			kv.setCurrentKey(2);
			IntValue default2 = kv.value();

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
