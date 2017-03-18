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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend}.
 */
public class MemoryStateBackendTest extends StateBackendTestBase<MemoryStateBackend> {

	@Override
	protected MemoryStateBackend getStateBackend() throws Exception {
		return new MemoryStateBackend(useAsyncMode());
	}

	protected boolean useAsyncMode() {
		return false;
	}

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}
	
	@Override
	@Test
	public void testMapStateRestoreWithWrongSerializers() {}

	@Test
	@SuppressWarnings("unchecked")
	public void testNumStateEntries() throws Exception {
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		HeapKeyedStateBackend<Integer> heapBackend = (HeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.update("foo");

		assertEquals(2, heapBackend.numStateEntries());

		backend.setCurrentKey(0);
		state.clear();

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.clear();

		assertEquals(0, heapBackend.numStateEntries());

		backend.dispose();
	}

	@Test
	public void testOversizedState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			try {
				CheckpointStreamFactory.CheckpointStateOutputStream outStream =
						streamFactory.createCheckpointStateOutputStream(12, 459);

				ObjectOutputStream oos = new ObjectOutputStream(outStream);
				oos.writeObject(state);

				oos.flush();

				outStream.closeAndGetHandle();

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
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(state);
			oos.flush();
			StreamStateHandle handle = os.closeAndGetHandle();

			assertNotNull(handle);

			try (ObjectInputStream ois = new ObjectInputStream(handle.openInputStream())) {
				assertEquals(state, ois.readObject());
				assertTrue(ois.available() <= 0);
			}
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
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
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

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}
}
