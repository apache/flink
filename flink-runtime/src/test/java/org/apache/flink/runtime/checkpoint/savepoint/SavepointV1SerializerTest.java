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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.TaskState;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that the Checkpoint Metadata V1 deserializer can deserialize a the metadata correctly
 * into the latest format.
 */
public class SavepointV1SerializerTest {

	@Test
	public void testSerializeDeserializeV1() throws Exception {
		final Random r = new Random(42);

		for (int i = 0; i < 50; ++i) {
			final long checkpointId = i + 123123;
			final Collection<TaskState> taskStates = CheckpointTestUtils.createTaskStates(r, 1 + r.nextInt(64), 1 + r.nextInt(64));

			// Serialize
			final ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			SavepointV1Serializer.serializeVersion1(checkpointId, taskStates, new DataOutputViewStreamWrapper(baos));
			final byte[] bytes = baos.toByteArray();

			// Deserialize
			final SavepointV2 actual = SavepointV1Serializer.INSTANCE.deserialize(
					new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
					Thread.currentThread().getContextClassLoader());

			assertEquals(checkpointId, actual.getCheckpointId());
			assertEquals(taskStates, actual.getTaskStates());
			assertTrue(actual.getMasterStates().isEmpty());
		}
	}
}
