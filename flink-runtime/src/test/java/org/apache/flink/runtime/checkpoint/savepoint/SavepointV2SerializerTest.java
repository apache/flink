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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SavepointV2SerializerTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	/**
	 * Tests that the relative path resolution returns sensible results.
	 */
	@Test
	public void testGetRelativePath() throws Exception {
		// Simple child
		verifyGetRelativePath("hdfs:///base", "hdfs:///base/child", "child");
		verifyGetRelativePath("hdfs:///base", "hdfs:///base/child/file", "child/file");

		// Not a child
		verifyGetRelativePath("hdfs:///base", "hdfs:///child-of-root", null);
		verifyGetRelativePath("hdfs:///base", "hdfs:///other-base/child", null);
		verifyGetRelativePath("hdfs:///base/child", "hdfs:///base", null);

		// Different schemes are not a child
		verifyGetRelativePath("hdfs:///base", "file:///base/child", null);
		verifyGetRelativePath("hdfs:///base", "file:///base/child/file", null);
	}

	/**
	 * Test serialization of {@link SavepointV2} instances.
	 */
	@Test
	public void testSerialization() throws Exception {
		Path ignoredBasePath = new Path("ignored");

		Random r = new Random(42);
		for (int i = 0; i < 100; ++i) {
			SavepointV2 expected = new SavepointV2(
				i+ 123123,
				SavepointV1Test.createTaskStates(1 + r.nextInt(64), 1 + r.nextInt(64)));

			SavepointV2Serializer serializer = SavepointV2Serializer.INSTANCE;

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

	/**
	 * Tests that file state handles serialized with this serializer can be
	 * relocated.
	 *
	 * <p>Serializes data to a file, creates a file state handle, serializes the
	 * handle, moves the data file to a new Folder, deserializes the handle with
	 * the new base path.
	 */
	@Test
	public void testSerializeDeserializeRelativeFileStateHandle() throws Exception {
		Path base = new Path(folder.newFolder("old-base").getAbsolutePath());
		Path newBase = new Path(folder.newFolder("new-base").getAbsolutePath());

		FileSystem fs = FileSystem.get(base.toUri());

		// The expected bytes
		byte[] expected = new byte[1024];
		ThreadLocalRandom.current().nextBytes(expected);

		// Write to a file
		FsCheckpointStateOutputStream fsOutStream = new FsCheckpointStateOutputStream(
			base, fs, 0, 0);
		fsOutStream.write(expected);

		// Get the handle and serialize it
		FileStateHandle fsHandle = (FileStateHandle) fsOutStream.closeAndGetHandle();

		SavepointV2Serializer serializer = SavepointV2Serializer.INSTANCE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serializeFileStreamStateHandle(fsHandle, base, new DataOutputStream(baos));

		// Move the file
		Path filePath = fsHandle.getFilePath();
		Path newFilePath = new Path(newBase, filePath.getName());

		assertTrue("Failed to move the file", fs.rename(filePath, newFilePath));

		// Deserialize the new file
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		FileStateHandle deserializeFsHandle = serializer.deserializeFileStreamStateHandle(newBase, new DataInputStream(bais));

		FSDataInputStream fsInStream = deserializeFsHandle.openInputStream();
		byte[] actual = new byte[expected.length];

		assertEquals(expected.length, fsInStream.read(actual));
		assertEquals(-1, fsInStream.read());

		assertArrayEquals(expected, actual);
	}

	private void verifyGetRelativePath(String base, String child, String expected) {
		Path basePath = new Path(base);
		Path childPath = new Path(child);
		Path relativePath = SavepointV2Serializer.getRelativePath(basePath, childPath);

		if (expected == null) {
			assertNull(relativePath);
		} else {
			assertNotNull(relativePath);
			assertEquals(new Path(expected), relativePath);
			assertFalse("Path should be relative: " + relativePath, relativePath.isAbsolute());
		}
	}
}
