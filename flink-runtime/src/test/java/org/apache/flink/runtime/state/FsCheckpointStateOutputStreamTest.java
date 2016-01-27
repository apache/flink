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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.*;

public class FsCheckpointStateOutputStreamTest {
	
	/** The temp dir, obtained in a platform neutral way */
	private static final Path TEMP_DIR_PATH = new Path(new File(System.getProperty("java.io.tmpdir")).toURI());
	
	
	@Test(expected = IllegalArgumentException.class)
	public void testWrongParameters() {
		// this should fail
		new FsStateBackend.FsCheckpointStateOutputStream(
			TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 4000, 5000);
	}


	@Test
	public void testEmptyState() throws Exception {
		StateBackend.CheckpointStateOutputStream stream = new FsStateBackend.FsCheckpointStateOutputStream(
			TEMP_DIR_PATH, FileSystem.getLocalFileSystem(), 1024, 512);
		
		StreamStateHandle handle = stream.closeAndGetHandle();
		assertTrue(handle instanceof ByteStreamStateHandle);
		
		InputStream inStream = handle.getState(ClassLoader.getSystemClassLoader());
		assertEquals(-1, inStream.read());
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
	
	private void runTest(int numBytes, int bufferSize, int threshold, boolean expectFile) throws Exception {
		StateBackend.CheckpointStateOutputStream stream = 
			new FsStateBackend.FsCheckpointStateOutputStream(
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
}
