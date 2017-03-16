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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.Test;

public class GenericSavepointSerializerTest {

	/**
	 * Test the generic serialization of savepoint instances without any overridden behaviour.
	 *
	 * <p>The abstract base serializer is used by the V1 and V2 serializer.
	 * The actual V1 serializer doesn't allow serialization of V1 savepoints
	 * anymore. Therefore we test the abstract behaviour here.
	 */
	@Test
	public void testGenericSerializeDeserialize() throws Exception {
		Path ignoredBasePath = new Path("ignored");

		Random r = new Random(42);
		for (int i = 0; i < 100; ++i) {
			SavepointV1 expected = new SavepointV1(
				i+ 123123,
				SavepointV1Test.createTaskStates(1 + r.nextInt(64), 1 + r.nextInt(64)));

			SavepointSerializer<SavepointV1> serializer = new GenericSavepointSerializer<SavepointV1>(
				new SavepointFactory<SavepointV1>() {
					@Override
					public SavepointV1 createSavepoint(long checkpointId,
						Collection<TaskState> taskStates) {
						return new SavepointV1(checkpointId, taskStates);
					}
				},
				new FileStateHandleSerializer() {
					@Override
					public void serializeFileStreamStateHandle(FileStateHandle fileStateHandle, Path basePath, DataOutputStream dos) throws IOException {
						throw new UnsupportedOperationException("Should not be called in this test");
					}

					@Override
					public FileStateHandle deserializeFileStreamStateHandle(Path basePath, DataInputStream dis) throws IOException {
						throw new UnsupportedOperationException("Should not be called in this test");
					}
				}
			);

			// Serialize
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			serializer.serialize(expected, ignoredBasePath, new DataOutputViewStreamWrapper(baos));
			byte[] bytes = baos.toByteArray();

			// Deserialize
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			Savepoint actual = serializer.deserialize(
				new DataInputViewStreamWrapper(bais),
				ignoredBasePath,
				Thread.currentThread().getContextClassLoader());

			assertEquals(expected, actual);
		}
	}

}
