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
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link FsCheckpointStateOutputStream}.
 */
public class FsCheckpointStateOutputStreamTest {

	@Rule
	public final TemporaryFolder tempDir = new TemporaryFolder();

	@Test(expected = IllegalArgumentException.class)
	public void testWrongParameters() throws Exception {
		// this should fail
		new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
			Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), 4000, 5000);
	}

	@Test
	public void testEmptyState() throws Exception {
		FsCheckpointStreamFactory.CheckpointStateOutputStream stream =
				new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
						Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), 1024, 512);

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
				new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
						Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), 31, 17);

		for (int i = 0; i < 64; ++i) {
			Assert.assertEquals(i, stream.getPos());
			stream.write(0x42);
		}

		stream.closeAndGetHandle();

		// ----------------------------------------------------

		stream = new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
				Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), 31, 17);

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
			Path.fromLocalFile(tempDir.newFolder()),
			fs,
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[] {1, 2, 3, 4, 5});

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
			Path.fromLocalFile(tempDir.newFolder()),
			fs,
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[] {1, 2, 3, 4, 5});

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
					Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), bufferSize, threshold);

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
				Path.fromLocalFile(tempDir.newFolder()), FileSystem.getLocalFileSystem(), 1024, 512);

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

	@Test
	public void testMixedBelowAndAboveThreshold() throws Exception {
		final byte[] state1 = new byte[1274673];
		final byte[] state2 = new byte[1];
		final byte[] state3 = new byte[0];
		final byte[] state4 = new byte[177];

		final Random rnd = new Random();
		rnd.nextBytes(state1);
		rnd.nextBytes(state2);
		rnd.nextBytes(state3);
		rnd.nextBytes(state4);

		final File directory = tempDir.newFolder();
		final Path basePath = Path.fromLocalFile(directory);

		final Supplier<CheckpointStateOutputStream> factory = () ->
				new FsCheckpointStateOutputStream(basePath, FileSystem.getLocalFileSystem(), 1024, 15);

		CheckpointStateOutputStream stream1 = factory.get();
		CheckpointStateOutputStream stream2 = factory.get();
		CheckpointStateOutputStream stream3 = factory.get();

		stream1.write(state1);
		stream2.write(state2);
		stream3.write(state3);

		FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
		ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
		ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

		// use with try-with-resources
		StreamStateHandle handle4;
		try (CheckpointStreamFactory.CheckpointStateOutputStream stream4 = factory.get()) {
			stream4.write(state4);
			handle4 = stream4.closeAndGetHandle();
		}

		// close before accessing handle
		CheckpointStreamFactory.CheckpointStateOutputStream stream5 = factory.get();
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
		assertFalse(isDirectoryEmpty(directory));
		ensureLocalFileDeleted(handle1.getFilePath());

		validateBytesInStream(handle2.openInputStream(), state2);
		handle2.discardState();
		assertFalse(isDirectoryEmpty(directory));

		// nothing was written to the stream, so it will return nothing
		assertNull(handle3);
		assertFalse(isDirectoryEmpty(directory));

		validateBytesInStream(handle4.openInputStream(), state4);
		handle4.discardState();
		assertTrue(isDirectoryEmpty(directory));
	}

	// ------------------------------------------------------------------------
	//  Not deleting parent directories
	// ------------------------------------------------------------------------

	/**
	 * This test checks that the stream does not check and clean the parent directory
	 * when encountering a write error.
	 */
	@Test
	public void testStreamDoesNotTryToCleanUpParentOnError() throws Exception {
		final File directory = tempDir.newFolder();

		// prevent creation of files in that directory
		assertTrue(directory.setWritable(false, true));
		checkDirectoryNotWritable(directory);

		FileSystem fs = spy(FileSystem.getLocalFileSystem());

		FsCheckpointStateOutputStream stream1 = new FsCheckpointStateOutputStream(
				Path.fromLocalFile(directory), fs, 1024, 1);

		FsCheckpointStateOutputStream stream2 = new FsCheckpointStateOutputStream(
				Path.fromLocalFile(directory), fs, 1024, 1);

		stream1.write(new byte[61]);
		stream2.write(new byte[61]);

		try {
			stream1.closeAndGetHandle();
			fail("this should fail with an exception");
		} catch (IOException ignored) {}

		stream2.close();

		// no delete call must have happened
		verify(fs, times(0)).delete(any(Path.class), anyBoolean());

		// the directory must still exist as a proper directory
		assertTrue(directory.exists());
		assertTrue(directory.isDirectory());
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

	private static boolean isDirectoryEmpty(File directory) {
		if (!directory.exists()) {
			return true;
		}
		String[] nested = directory.list();
		return nested == null || nested.length == 0;
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

	private static void checkDirectoryNotWritable(File directory) {
		try {
			try (FileOutputStream fos = new FileOutputStream(new File(directory, "temp"))) {
				fos.write(42);
				fos.flush();
			}

			fail("this should fail when writing is properly prevented");
		} catch (IOException ignored) {
			// expected, works
		}
	}
}
