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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

public class SavepointV01SerializerTest {

	/**
	 * Test serialization of {@link SavepointV0} instance.
	 */
	@Test
	public void testSerializeDeserializeV1() throws Exception {
		SavepointV0 expected = new SavepointV0(123123, SavepointV01Test.createTaskStates(8, 32));

		SavepointV1Serializer serializer = SavepointV1Serializer.INSTANCE;

		// Serialize
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(expected, new DataOutputViewStreamWrapper(baos));
		byte[] bytes = baos.toByteArray();

		// Deserialize
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		Savepoint actual = serializer.deserialize(new DataInputViewStreamWrapper(bais));

		assertEquals(expected, actual);
	}
}
