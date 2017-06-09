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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SavepointV1SerializerTest {

	/**
	 * Test serialization of {@link SavepointV1} instance.
	 */
	@Test
	public void testSerializeDeserializeV1() throws Exception {
		final Random r = new Random(42);

		for (int i = 0; i < 50; ++i) {
			SavepointV1 expected =
					new SavepointV1(i+ 123123, CheckpointTestUtils.createTaskStates(r, 1 + r.nextInt(64), 1 + r.nextInt(64)));

			SavepointV1Serializer serializer = SavepointV1Serializer.INSTANCE;

			// Serialize
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			serializer.serializeOld(expected, new DataOutputViewStreamWrapper(baos));
			byte[] bytes = baos.toByteArray();

			// Deserialize
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			SavepointV2 actual = serializer.deserialize(
					new DataInputViewStreamWrapper(bais),
					Thread.currentThread().getContextClassLoader());


			assertEquals(expected.getCheckpointId(), actual.getCheckpointId());
			assertEquals(expected.getTaskStates(), actual.getTaskStates());
			assertTrue(actual.getMasterStates().isEmpty());
		}
	}
}
