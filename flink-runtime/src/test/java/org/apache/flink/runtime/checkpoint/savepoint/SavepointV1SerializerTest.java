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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.Test;

public class SavepointV1SerializerTest {

	/**
	 * Test deserialization of {@link SavepointV1} instances.
	 */
	@Test
	public void testDeserialize() throws Exception {
		// The actual SavepointV1Serializer doesn't allow serialization
		// any longer, use this one. This also has the benefit of "fixing"
		// the layout against accidental changes in the actual serialier.

		SavepointSerializer<SavepointV1> legacySerializer = new GenericSavepointSerializer<SavepointV1>(
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
					dos.writeLong(fileStateHandle.getStateSize());
					dos.writeUTF(fileStateHandle.getFilePath().toString());
				}

				@Override
				public FileStateHandle deserializeFileStreamStateHandle(Path basePath, DataInputStream dis) throws IOException {
					throw new UnsupportedOperationException("Should not be called in test");
				}
			}
		);

		Path ignoredBasePath = new Path("ignored");

		Random r = new Random(42);
		for (int i = 0; i < 100; ++i) {
			SavepointV1 expected = new SavepointV1(
				i+ 123123,
				SavepointV1Test.createTaskStates(1 + r.nextInt(64), 1 + r.nextInt(64)));

			SavepointV1Serializer serializer = SavepointV1Serializer.INSTANCE;

			// Serialize
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			legacySerializer.serialize(expected, ignoredBasePath, new DataOutputViewStreamWrapper(baos));
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

	@Test(expected = UnsupportedOperationException.class)
	public void testSerializeUnsupportedOperationException() throws Exception {
		SavepointV1Serializer serializer = SavepointV1Serializer.INSTANCE;

		serializer.serialize(
			new SavepointV1(0, Collections.<TaskState>emptyList()),
			new Path("ignored"),
			new DataOutputStream(new ByteArrayOutputStream(1)));
	}
}
