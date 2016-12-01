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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FsCheckpointStateOutputStreamTest {

	/** The temp dir */
	private Path tempDirPath = null;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		tempDirPath = new Path(temporaryFolder.newFolder().toURI());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongParameters() throws IOException {
		// this should fail
		new FsStateBackend.FsCheckpointStateOutputStream(
			tempDirPath, FileSystem.getLocalFileSystem(), new HashSet<FsCheckpointStateOutputStream>(), 4000, 5000);
	}

	@Test
	public void testEmptyState() throws Exception {
		AbstractStateBackend.CheckpointStateOutputStream stream = new FsStateBackend.FsCheckpointStateOutputStream(
			tempDirPath, FileSystem.getLocalFileSystem(), new HashSet<FsCheckpointStateOutputStream>(), 1024, 512);
		
		StreamStateHandle handle = stream.closeAndGetHandle();
		assertTrue(handle instanceof ByteStreamStateHandle);
		
		InputStream inStream = handle.getState(ClassLoader.getSystemClassLoader());
		assertEquals(-1, inStream.read());
	}

	@Test
	public void testCloseAndGetPath() throws Exception {
		FsCheckpointStateOutputStream stream = new FsCheckpointStateOutputStream(
				tempDirPath,
				FileSystem.getLocalFileSystem(),
				new HashSet<FsCheckpointStateOutputStream>(),
				1024,
				512);

		stream.write(1);

		Path path = stream.closeAndGetPath();
		assertNotNull(path);

		// cleanup
		FileSystem.getLocalFileSystem().delete(path, false);
	}

	@Test
	public void testWriteFailsFastWhenClosed() throws Exception {
		final HashSet<FsCheckpointStateOutputStream> openStreams = new HashSet<>();

		FsCheckpointStateOutputStream stream1 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		FsCheckpointStateOutputStream stream2 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		FsCheckpointStateOutputStream stream3 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		assertFalse(stream1.isClosed());
		assertFalse(stream2.isClosed());
		assertFalse(stream3.isClosed());

		// simple close
		stream1.close();

		// close with handle
		StreamStateHandle handle = stream2.closeAndGetHandle();

		// close with path
		Path path = stream3.closeAndGetPath();

		assertTrue(stream1.isClosed());
		assertTrue(stream2.isClosed());
		assertTrue(stream3.isClosed());

		validateStreamNotWritable(stream1);
		validateStreamNotWritable(stream2);
		validateStreamNotWritable(stream3);

		// clean up
		handle.discardState();
		FileSystem.getLocalFileSystem().delete(path, false);
	}

	@Test
	public void testAddAndRemoveFromOpenStreamsSet() throws Exception {
		final HashSet<FsCheckpointStateOutputStream> openStreams = new HashSet<>();

		FsCheckpointStateOutputStream stream1 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		FsCheckpointStateOutputStream stream2 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		FsCheckpointStateOutputStream stream3 = new FsCheckpointStateOutputStream(
				tempDirPath, FileSystem.getLocalFileSystem(), openStreams, 1024, 512);

		assertTrue(openStreams.contains(stream1));
		assertTrue(openStreams.contains(stream2));
		assertTrue(openStreams.contains(stream3));
		assertEquals(3, openStreams.size());

		// simple close
		stream1.close();

		// close with handle
		StreamStateHandle handle = stream2.closeAndGetHandle();

		// close with path
		Path path = stream3.closeAndGetPath();

		assertFalse(openStreams.contains(stream1));
		assertFalse(openStreams.contains(stream2));
		assertFalse(openStreams.contains(stream3));
		assertEquals(0, openStreams.size());

		// clean up
		handle.discardState();
		FileSystem.getLocalFileSystem().delete(path, false);
	}

	@Test
	public void testStateBlowMemThreshold() throws Exception {
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

	/**
	 * Tests that the underlying stream file is deleted upon calling close.
	 */
	@Test
	public void testCleanupWhenClosingStream() throws IOException {

		final FileSystem fs = mock(FileSystem.class);
		final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

		final ArgumentCaptor<Path>  pathCaptor = ArgumentCaptor.forClass(Path.class);

		when(fs.create(pathCaptor.capture(), anyBoolean())).thenReturn(outputStream);

		AbstractStateBackend.CheckpointStateOutputStream stream = new FsStateBackend.FsCheckpointStateOutputStream(
			tempDirPath,
			fs,
			new HashSet<FsCheckpointStateOutputStream>(),
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[]{1,2,3,4,5});

		verify(fs).create(any(Path.class), anyBoolean());

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

		when(fs.create(pathCaptor.capture(), anyBoolean())).thenReturn(outputStream);
		doThrow(new IOException("Test IOException.")).when(outputStream).close();

		AbstractStateBackend.CheckpointStateOutputStream stream = new FsStateBackend.FsCheckpointStateOutputStream(
			tempDirPath,
			fs,
			new HashSet<FsCheckpointStateOutputStream>(),
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[]{1,2,3,4,5});

		verify(fs).create(any(Path.class), anyBoolean());

		try {
			stream.closeAndGetHandle();
			fail("Expected IOException");
		} catch (IOException ioE) {
			// expected exception
		}

		verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
	}

	/**
	 * Tests that the underlying stream file is deleted if the closeAndGetPath method fails.
	 */
	@Test
	public void testCleanupWhenFailingCloseAndGetPath() throws IOException {
		final FileSystem fs = mock(FileSystem.class);
		final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

		final ArgumentCaptor<Path>  pathCaptor = ArgumentCaptor.forClass(Path.class);

		when(fs.create(pathCaptor.capture(), anyBoolean())).thenReturn(outputStream);
		doThrow(new IOException("Test IOException.")).when(outputStream).close();

		FsStateBackend.FsCheckpointStateOutputStream stream = new FsStateBackend.FsCheckpointStateOutputStream(
			tempDirPath,
			fs,
			new HashSet<FsCheckpointStateOutputStream>(),
			4,
			0);

		// this should create the underlying file stream
		stream.write(new byte[]{1,2,3,4,5});

		verify(fs).create(any(Path.class), anyBoolean());

		try {
			stream.closeAndGetPath();
			fail("Expected IOException");
		} catch (IOException ioE) {
			// expected exception
		}

		verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
	}
	
	private void runTest(int numBytes, int bufferSize, int threshold, boolean expectFile) throws Exception {
		AbstractStateBackend.CheckpointStateOutputStream stream =
			new FsStateBackend.FsCheckpointStateOutputStream(
					tempDirPath, FileSystem.getLocalFileSystem(),
					new HashSet<FsCheckpointStateOutputStream>(), bufferSize, threshold);
		
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
			assertTrue(handle instanceof FileStreamStateHandle);
		} else {
			assertTrue(handle instanceof ByteStreamStateHandle);
		}

		// make sure the writing process did not alter the original byte array
		assertArrayEquals(original, bytes);

		InputStream inStream = handle.getState(ClassLoader.getSystemClassLoader());
		byte[] validation = new byte[bytes.length];
		int bytesRead = inStream.read(validation);

		assertEquals(numBytes, bytesRead);
		assertEquals(-1, inStream.read());

		assertArrayEquals(bytes, validation);
		
		handle.discardState();
	}

	private void validateStreamNotWritable(FsCheckpointStateOutputStream stream) {
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
