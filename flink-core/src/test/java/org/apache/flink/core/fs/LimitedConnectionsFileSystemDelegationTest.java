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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.FileSystem.WriteMode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that the method delegation works properly the {@link LimitedConnectionsFileSystem}
 * and its created input and output streams.
 */
public class LimitedConnectionsFileSystemDelegationTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	@SuppressWarnings("deprecation")
	public void testDelegateFsMethods() throws IOException {
		final FileSystem fs = mock(FileSystem.class);
		when(fs.open(any(Path.class))).thenReturn(mock(FSDataInputStream.class));
		when(fs.open(any(Path.class), anyInt())).thenReturn(mock(FSDataInputStream.class));
		when(fs.create(any(Path.class), anyBoolean())).thenReturn(mock(FSDataOutputStream.class));
		when(fs.create(any(Path.class), any(WriteMode.class))).thenReturn(mock(FSDataOutputStream.class));
		when(fs.create(any(Path.class), anyBoolean(), anyInt(), anyShort(), anyLong())).thenReturn(mock(FSDataOutputStream.class));

		final LimitedConnectionsFileSystem lfs = new LimitedConnectionsFileSystem(fs, 1000);
		final Random rnd = new Random();

		lfs.isDistributedFS();
		verify(fs).isDistributedFS();

		lfs.getWorkingDirectory();
		verify(fs).isDistributedFS();

		lfs.getHomeDirectory();
		verify(fs).getHomeDirectory();

		lfs.getUri();
		verify(fs).getUri();

		{
			Path path = mock(Path.class);
			lfs.getFileStatus(path);
			verify(fs).getFileStatus(path);
		}

		{
			FileStatus path = mock(FileStatus.class);
			int pos = rnd.nextInt();
			int len = rnd.nextInt();
			lfs.getFileBlockLocations(path, pos, len);
			verify(fs).getFileBlockLocations(path, pos, len);
		}

		{
			Path path = mock(Path.class);
			int bufferSize = rnd.nextInt();
			lfs.open(path, bufferSize);
			verify(fs).open(path, bufferSize);
		}

		{
			Path path = mock(Path.class);
			lfs.open(path);
			verify(fs).open(path);
		}

		lfs.getDefaultBlockSize();
		verify(fs).getDefaultBlockSize();

		{
			Path path = mock(Path.class);
			lfs.listStatus(path);
			verify(fs).listStatus(path);
		}

		{
			Path path = mock(Path.class);
			lfs.exists(path);
			verify(fs).exists(path);
		}

		{
			Path path = mock(Path.class);
			boolean recursive = rnd.nextBoolean();
			lfs.delete(path, recursive);
			verify(fs).delete(path, recursive);
		}

		{
			Path path = mock(Path.class);
			lfs.mkdirs(path);
			verify(fs).mkdirs(path);
		}

		{
			Path path = mock(Path.class);
			boolean overwrite = rnd.nextBoolean();
			int bufferSize = rnd.nextInt();
			short replication = (short) rnd.nextInt();
			long blockSize = rnd.nextInt();

			lfs.create(path, overwrite, bufferSize, replication, blockSize);
			verify(fs).create(path, overwrite, bufferSize, replication, blockSize);
		}

		{
			Path path = mock(Path.class);
			WriteMode mode = rnd.nextBoolean() ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;
			lfs.create(path, mode);
			verify(fs).create(path, mode);
		}

		{
			Path path1 = mock(Path.class);
			Path path2 = mock(Path.class);
			lfs.rename(path1, path2);
			verify(fs).rename(path1, path2);
		}

		{
			FileSystemKind kind = rnd.nextBoolean() ? FileSystemKind.FILE_SYSTEM : FileSystemKind.OBJECT_STORE;
			when(fs.getKind()).thenReturn(kind);
			assertEquals(kind, lfs.getKind());
			verify(fs).getKind();
		}
	}

	@Test
	public void testDelegateOutStreamMethods() throws IOException {

		// mock the output stream
		final FSDataOutputStream mockOut = mock(FSDataOutputStream.class);
		final long outPos = 46651L;
		when(mockOut.getPos()).thenReturn(outPos);

		final FileSystem fs = mock(FileSystem.class);
		when(fs.create(any(Path.class), any(WriteMode.class))).thenReturn(mockOut);

		final LimitedConnectionsFileSystem lfs = new LimitedConnectionsFileSystem(fs, 100);
		final FSDataOutputStream out = lfs.create(mock(Path.class), WriteMode.OVERWRITE);

		// validate the output stream

		out.write(77);
		verify(mockOut).write(77);

		{
			byte[] bytes = new byte[1786];
			out.write(bytes, 100, 111);
			verify(mockOut).write(bytes, 100, 111);
		}

		assertEquals(outPos, out.getPos());

		out.flush();
		verify(mockOut).flush();

		out.sync();
		verify(mockOut).sync();

		out.close();
		verify(mockOut).close();
	}

	@Test
	public void testDelegateInStreamMethods() throws IOException {
		// mock the input stream
		final FSDataInputStream mockIn = mock(FSDataInputStream.class);
		final int value = 93;
		final int bytesRead = 11;
		final long inPos = 93;
		final int available = 17;
		final boolean markSupported = true;
		when(mockIn.read()).thenReturn(value);
		when(mockIn.read(any(byte[].class), anyInt(), anyInt())).thenReturn(11);
		when(mockIn.getPos()).thenReturn(inPos);
		when(mockIn.available()).thenReturn(available);
		when(mockIn.markSupported()).thenReturn(markSupported);

		final FileSystem fs = mock(FileSystem.class);
		when(fs.open(any(Path.class))).thenReturn(mockIn);

		final LimitedConnectionsFileSystem lfs = new LimitedConnectionsFileSystem(fs, 100);
		final FSDataInputStream in = lfs.open(mock(Path.class));

		// validate the input stream

		assertEquals(value, in.read());
		assertEquals(bytesRead, in.read(new byte[11], 2, 5));

		assertEquals(inPos, in.getPos());

		in.seek(17876);
		verify(mockIn).seek(17876);

		assertEquals(available, in.available());

		assertEquals(markSupported, in.markSupported());

		in.mark(9876);
		verify(mockIn).mark(9876);

		in.close();
		verify(mockIn).close();
	}
}
