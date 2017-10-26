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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.mockito.ArgumentCaptor;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link }.
 */
public class FixFileFsStateOutputStreamTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Validates that even empty streams create a file and a file state handle.
	 */
	@Test
	public void testEmptyState() throws Exception {
		final FileSystem fs = FileSystem.getLocalFileSystem();
		final Path path = new Path(new Path(tmp.newFolder().toURI()), "myFileName");

		final FileStateHandle handle;
		try (FixFileFsStateOutputStream stream = new FixFileFsStateOutputStream(fs, path)) {
			handle = stream.closeAndGetHandle();
		}

		// must have created a handle
		assertNotNull(handle);
		assertEquals(path, handle.getFilePath());

		// the pointer path should exist as a directory
		assertTrue(fs.exists(handle.getFilePath()));
		assertFalse(fs.getFileStatus(path).isDir());

		// the contents should be empty
		try (FSDataInputStream in = handle.openInputStream()) {
			assertEquals(-1, in.read());
		}
	}

	/**
	 * Simple write and read test
	 */
	@Test
	public void testWriteAndRead() throws Exception {
		final FileSystem fs = FileSystem.getLocalFileSystem();
		final Path path = new Path(new Path(tmp.newFolder().toURI()), "fooBarName");

		final Random rnd = new Random();
		final byte[] data = new byte[1694523];

		// write the data (mixed single byte writes and array writes)
		final FileStateHandle handle;
		try (FixFileFsStateOutputStream stream = new FixFileFsStateOutputStream(fs, path)) {
			for (int i = 0; i < data.length;) {
				if (rnd.nextBoolean()) {
					stream.write(data[i++]);
				}
				else {
					int len = rnd.nextInt(Math.min(data.length - i, 32));
					stream.write(data, i, len);
					i += len;
				}
			}
			handle = stream.closeAndGetHandle();
		}

		// (1) stream from handle must hold the contents
		try (FSDataInputStream in = handle.openInputStream()) {
			byte[] buffer = new byte[data.length];
			readFully(in, buffer);
			assertArrayEquals(data, buffer);
		}

		// (2) the pointer must point to a file with that contents
		try (FSDataInputStream in = fs.open(handle.getFilePath())) {
			byte[] buffer = new byte[data.length];
			readFully(in, buffer);
			assertArrayEquals(data, buffer);
		}
	}

	/**
	 * Tests that the underlying stream file is deleted upon calling close.
	 */
	@Test
	public void testCleanupWhenClosingStream() throws IOException {
		final FileSystem fs = FileSystem.getLocalFileSystem();
		final Path path = new Path(new Path(tmp.newFolder().toURI()), "nonCreativeTestFileName");

		// write some test data and close the stream
		try (FixFileFsStateOutputStream stream = new FixFileFsStateOutputStream(fs, path)) {
			Random rnd = new Random();
			for (int i = 0; i < rnd.nextInt(1000); i++) {
				stream.write(rnd.nextInt(100));
			}
			assertTrue(fs.exists(path));
		}

		assertFalse(fs.exists(path));
	}

	/**
	 * Tests that the underlying stream file is deleted if the closeAndGetHandle method fails.
	 */
	@Test
	public void testCleanupWhenFailingCloseAndGetHandle() throws IOException {
		final Path path = new Path(new Path(tmp.newFolder().toURI()), "neverCreated");

		final FileSystem fs = mock(FileSystem.class);
		final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

		final ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

		when(fs.create(pathCaptor.capture(), any(WriteMode.class))).thenReturn(outputStream);
		doThrow(new IOException("Test IOException.")).when(outputStream).close();

		FixFileFsStateOutputStream stream = new FixFileFsStateOutputStream(fs, path);
		verify(fs).create(any(Path.class), any(WriteMode.class));

		stream.write(new byte[] {1,2,3,4,5});

		try {
			stream.closeAndGetHandle();
			fail("Expected IOException");
		}
		catch (IOException ignored) {
			// expected exception
		}

		verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
	}

	/**
	 * This test validates that a close operation can happen even while a 'closeAndGetHandle()'
	 * call is in progress.
	 *
	 * <p>That behavior is essential for fast cancellation (concurrent cleanup).
	 */
	@Test
	public void testCloseDoesNotLock() throws Exception {
		// a stream that blocks but is released when closed
		final FSDataOutputStream stream = new BlockerStream();

		final FileSystem fileSystem = mock(FileSystem.class);
		when(fileSystem.create(any(Path.class), any(WriteMode.class))).thenReturn(stream);

		final Path path = new Path(new Path(tmp.newFolder().toURI()), "this-is-ignored-anyways.file");
		final FixFileFsStateOutputStream checkpointStream =
				new FixFileFsStateOutputStream(fileSystem, path);

		final OneShotLatch sync = new OneShotLatch();

		final CheckedThread thread = new CheckedThread() {

			@Override
			public void go() throws Exception {
				sync.trigger();
				// that call should now block, because it accesses the position
				checkpointStream.closeAndGetHandle();
			}
		};
		thread.start();

		sync.await();
		checkpointStream.close();

		// the thread may or may not fail, that depends on the thread race
		// it is not important for this test, important is that the thread does not freeze/lock up
		try {
			thread.sync();
		} catch (IOException ignored) {}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static void readFully(InputStream in, byte[] buffer) throws IOException {
		int pos = 0;
		int remaining = buffer.length;

		while (remaining > 0) {
			int read = in.read(buffer, pos, remaining);
			if (read == -1) {
				throw new EOFException();
			}

			pos += read;
			remaining -= read;
		}
	}

	private static class BlockerStream extends FSDataOutputStream {

		private final OneShotLatch blocker = new OneShotLatch();

		@Override
		public long getPos() throws IOException {
			block();
			return 0L;
		}

		@Override
		public void write(int b) throws IOException {
			block();
		}

		@Override
		public void flush() throws IOException {
			block();
		}

		@Override
		public void sync() throws IOException {
			block();
		}

		@Override
		public void close() throws IOException {
			blocker.trigger();
		}

		private void block() throws IOException {
			try {
				blocker.await();
			} catch (InterruptedException e) {
				throw new IOException("interrupted");
			}
			throw new IOException("closed");
		}
	}
}
