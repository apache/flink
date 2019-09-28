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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;

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
 * Tests for the {@link MemoryCheckpointOutputStream}.
 */
public class MemoryCheckpointOutputStreamTest {

	@Test
	public void testOversizedState() throws Exception {
		HashMap<String, Integer> state = new HashMap<>();
		state.put("hey there", 2);
		state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

		CheckpointStateOutputStream outStream = new MemoryCheckpointOutputStream(10);
		ObjectOutputStream oos = new ObjectOutputStream(outStream);

		oos.writeObject(state);
		oos.flush();

		try {
			outStream.closeAndGetHandle();
			fail("this should cause an exception");
		}
		catch (IOException e) {
			// that's what we expect
		}
	}

	@Test
	public void testStateStream() throws Exception {
		HashMap<String, Integer> state = new HashMap<>();
		state.put("hey there", 2);
		state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

		CheckpointStateOutputStream outStream = new MemoryCheckpointOutputStream(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE);
		ObjectOutputStream oos = new ObjectOutputStream(outStream);
		oos.writeObject(state);
		oos.flush();

		StreamStateHandle handle = outStream.closeAndGetHandle();
		assertNotNull(handle);

		try (ObjectInputStream ois = new ObjectInputStream(handle.openInputStream())) {
			assertEquals(state, ois.readObject());
			assertTrue(ois.available() <= 0);
		}
	}
}
