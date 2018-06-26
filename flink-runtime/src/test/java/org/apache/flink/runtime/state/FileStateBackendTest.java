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

import java.nio.file.FileSystems;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileStateBackendTest extends StateBackendTestBase<FsStateBackend> {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected FsStateBackend getStateBackend() throws Exception {
		File checkpointPath = tempFolder.newFolder();
		return new FsStateBackend(localFileUri(checkpointPath), useAsyncMode());
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

			// nothing was written to the stream, so it will return nothing
			assertNull(handle3);

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEntropyStateOutputStream() throws IOException {
		File basePath = tempFolder.newFolder().getAbsoluteFile();
		final String path = basePath.getAbsolutePath();
		final String entropyKey = "_ENTROPY_";

		final String pathWithEntropy = path + FileSystems.getDefault().getSeparator() + entropyKey;

		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(new File(pathWithEntropy).toURI(), 15, entropyKey));
			JobID jobId = new JobID();

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			File checkpointPath = new File(pathWithEntropy, jobId.toString());

			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_entropy");

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523453L;

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

			// nothing was written to the stream, so it will return nothing
			assertNull(handle3);

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEntropyCheckpointStream() throws Exception {
		File basePath = tempFolder.newFolder().getAbsoluteFile();
		{
			final String path = basePath.getAbsolutePath();
			final String entropyKey = "_ENTROPY_";

			verifyEntropy(path, entropyKey, false);
		}

		{
			final String path = basePath.getAbsolutePath();
			final String entropyKey = "";

			verifyEntropy(path, entropyKey, false);
		}
	}

	@Test
	public void testEntropyInvalidCharsCheckpointStream() throws Exception {
		File basePath = tempFolder.newFolder().getAbsoluteFile();
		{
			final String path = basePath.getAbsolutePath();
			final String entropyKey = "#$**!^\\";

			verifyEntropy(path, entropyKey, true);
		}

		{
			final String path = basePath.getAbsolutePath();
			final String entropyKey = "//\\";

			verifyEntropy(path, entropyKey, true);
		}
	}

	private void verifyEntropy(String path, String entropyKey, boolean invalid) {
		//add the entropy pattern at the end of the path
		final String pathWithEntropy = path + FileSystems.getDefault().getSeparator() + entropyKey;

		try {
			// the state backend has a very low in-mem state threshold (15 bytes)
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(new File(pathWithEntropy).toURI(), 15, entropyKey));
			JobID jobId = new JobID();

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			File checkpointPath = new File(pathWithEntropy, jobId.toString());

			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_entropy");
			final Path chkpointPath = ((FsCheckpointStreamFactory)streamFactory).getCheckpointDirectory();

			if (entropyKey == null || entropyKey.isEmpty()) {
				assert(true);
				return;
			}
			//check entropyKey got replaced by the actual 4 character random alphanumeric string
			assert(!chkpointPath.getPath().contains(entropyKey));
			final String[] segments = chkpointPath.getPath().split(FileSystems.getDefault().getSeparator());

			//jobid gets suffixed after the entropy
			final String entropy = segments[segments.length - 2];
			assert(entropy.length() == 4);
			assert(StringUtils.isAlphanumeric(entropy));

		}
		catch (Exception e) {
			e.printStackTrace();
			if (!invalid) {
				fail(e.getMessage());
			} else {
				assert(true);
			}
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
		try {
			byte[] holder = new byte[data.length];

			int pos = 0;
			int read;
			while (pos < holder.length && (read = is.read(holder, pos, holder.length - pos)) != -1) {
				pos += read;
			}

			assertEquals("not enough data", holder.length, pos);
			assertEquals("too much data", -1, is.read());
			assertArrayEquals("wrong data", data, holder);
		} finally {
			is.close();
		}
	}

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}

}
