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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FsCheckpointStateOutputStreamTest {

	/** The temp dir, obtained in a platform neutral way */
	private static final Path TEMP_DIR_PATH = new Path(new File(System.getProperty("java.io.tmpdir")).toURI());


	@Test(expected = IllegalArgumentException.class)
	public void testWrongParameters() {
		// this should fail
		new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
			TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 4000, 5000);
	}


	@Test
	public void testEmptyState() throws Exception {
		FsCheckpointStreamFactory.CheckpointStateOutputStream stream =
				new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 1024, 512);

		StreamStateHandle handle = stream.closeAndGetHandle();
		assertTrue(handle == null);
	}

	@Test
	public void testStateBelowMemThreshold() throws Exception {
		runTest(222, 999, 512, false);
	}

	@Test
	public void testStateOneBufferAboveThreshold() throws Exception {
		runTest(896, 1024, 15, true);
	}

	@Test
	public void testStateAboveMemThreshold() throws Exception {
		runTest(576446, 259, 17, true);
	}

	@Test
	public void testZeroThreshold() throws Exception {
		runTest(16678, 4096, 0, true);
	}

	@Test
	public void testGetPos() throws Exception {
		FsCheckpointStreamFactory.CheckpointStateOutputStream stream =
				new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 31, 17);

		for (int i = 0; i < 64; ++i) {
			Assert.assertEquals(i, stream.getPos());
			stream.write(0x42);
		}

		stream.closeAndGetHandle();

		// ----------------------------------------------------

		stream = new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 31, 17);

		byte[] data = "testme!".getBytes(ConfigConstants.DEFAULT_CHARSET);

		for (int i = 0; i < 7; ++i) {
			Assert.assertEquals(i * (1 + data.length), stream.getPos());
			stream.write(0x42);
			stream.write(data);
		}

		stream.closeAndGetHandle();
	}

	/**
	 * Tests that the underlying stream file is deleted upon calling close.
	 */
	@Test
	public void testCleanupWhenClosingStream() throws IOException {

		final FileSystem fs = mock(FileSystem.class);
		final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

		final ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

		when(fs.create(pathCaptor.capture(), any(FileSystem.WriteMode.class))).thenReturn(outputStream);

		CheckpointStreamFactory.CheckpointStateOutputStream stream = new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
			TEMP_DIR_PATH,
			fs,
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[]{1,2,3,4,5});

		verify(fs).create(any(Path.class), any(FileSystem.WriteMode.class));

		stream.close();

		verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
	}

	/**
	 * Tests that the underlying stream file is deleted if the closeAndGetHandle method fails.
	 */
	@Test
	public void testCleanupWhenFailingCloseAndGetHandle() throws IOException {
		final FileSystem fs = mock(FileSystem.class);
		final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

		final ArgumentCaptor<Path>  pathCaptor = ArgumentCaptor.forClass(Path.class);

		when(fs.create(pathCaptor.capture(), any(FileSystem.WriteMode.class))).thenReturn(outputStream);
		doThrow(new IOException("Test IOException.")).when(outputStream).close();

		CheckpointStreamFactory.CheckpointStateOutputStream stream = new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
			TEMP_DIR_PATH,
			fs,
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[]{1,2,3,4,5});

		verify(fs).create(any(Path.class), any(FileSystem.WriteMode.class));

		try {
			stream.closeAndGetHandle();
			fail("Expected IOException");
		} catch (IOException ioE) {
			// expected exception
		}

		verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
	}

	private void runTest(int numBytes, int bufferSize, int threshold, boolean expectFile) throws Exception {
		FsCheckpointStreamFactory.CheckpointStateOutputStream stream =
			new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
				TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), bufferSize, threshold);

		Random rnd = new Random();
		byte[] original = new byte[numBytes];
		byte[] bytes = new byte[original.length];

		rnd.nextBytes(original);
		System.arraycopy(original, 0, bytes, 0, original.length);

		// the test writes a mixture of writing individual bytes and byte arrays
		int pos = 0;
		while (pos < bytes.length) {
			boolean single = rnd.nextBoolean();
			if (single) {
				stream.write(bytes[pos++]);
			}
			else {
				int num = rnd.nextInt(Math.min(10, bytes.length - pos));
				stream.write(bytes, pos, num);
				pos += num;
			}
		}

		StreamStateHandle handle = stream.closeAndGetHandle();
		if (expectFile) {
			assertTrue(handle instanceof FileStateHandle);
		} else {
			assertTrue(handle instanceof ByteStreamStateHandle);
		}

		// make sure the writing process did not alter the original byte array
		assertArrayEquals(original, bytes);

		try (InputStream inStream = handle.openInputStream()) {
			byte[] validation = new byte[bytes.length];

			DataInputStream dataInputStream = new DataInputStream(inStream);
			dataInputStream.readFully(validation);

			assertArrayEquals(bytes, validation);
		}

		handle.discardState();
	}

	@Test
	public void testWriteFailsFastWhenClosed() throws Exception {
		FsCheckpointStateOutputStream stream = new FsCheckpointStateOutputStream(
				TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 1024, 512);

		assertFalse(stream.isClosed());

		stream.close();
		assertTrue(stream.isClosed());

		try {
			stream.write(1);
			fail();
		} catch (IOException e) {
			// expected
		}

		try {
			stream.write(new byte[4], 1, 2);
			fail();
		} catch (IOException e) {
			// expected
		}
	}
}
