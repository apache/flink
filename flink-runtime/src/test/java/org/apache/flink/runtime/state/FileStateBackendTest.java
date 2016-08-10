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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

public class FileStateBackendTest extends StateBackendTestBase<FsStateBackend> {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected FsStateBackend getStateBackend() throws Exception {
		File checkpointPath = tempFolder.newFolder();
		return new FsStateBackend(localFileUri(checkpointPath));
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

	@Test
	public void testStateOutputStream() throws IOException {
		File basePath = tempFolder.newFolder().getAbsoluteFile();

		try {
			// the state backend has a very low in-mem state threshold (15 bytes)
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(basePath.toURI(), 15));
			JobID jobId = new JobID();

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			File checkpointPath = new File(basePath.getAbsolutePath(), jobId.toString());

			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_op");

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523452L;

			CheckpointStreamFactory.CheckpointStateOutputStream stream1 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream2 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream3 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			StreamStateHandle handle4;
			try (CheckpointStreamFactory.CheckpointStateOutputStream stream4 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = stream4.closeAndGetHandle();
			}

			// close before accessing handle
			CheckpointStreamFactory.CheckpointStateOutputStream stream5 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.openInputStream(), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(basePath));
			ensureLocalFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.openInputStream(), state2);
			handle2.discardState();

			validateBytesInStream(handle3.openInputStream(), state3);
			handle3.discardState();

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void ensureLocalFileDeleted(Path path) {
		URI uri = path.toUri();
		if ("file".equals(uri.getScheme())) {
			File file = new File(uri.getPath());
			assertFalse("file not properly deleted", file.exists());
		}
		else {
			throw new IllegalArgumentException("not a local path");
		}
	}

	private static void deleteDirectorySilently(File dir) {
		try {
			FileUtils.deleteDirectory(dir);
		}
		catch (IOException ignored) {}
	}

	private static boolean isDirectoryEmpty(File directory) {
		if (!directory.exists()) {
			return true;
		}
		String[] nested = directory.list();
		return nested == null || nested.length == 0;
	}

	private static String localFileUri(File path) {
		return path.toURI().toString();
	}

	private static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
		byte[] holder = new byte[data.length];

		int pos = 0;
		int read;
		while (pos < holder.length && (read = is.read(holder, pos, holder.length - pos)) != -1) {
			pos += read;
		}

		assertEquals("not enough data", holder.length, pos);
		assertEquals("too much data", -1, is.read());
		assertArrayEquals("wrong data", data, holder);
		is.close();
	}

	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}

}
