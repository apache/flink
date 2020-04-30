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

import org.apache.flink.core.fs.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FileStateHandle}.
 */
public class FileStateHandleTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testDisposeDeletesFile() throws Exception {
		File file = tempFolder.newFile();
		writeTestData(file);
		assertTrue(file.exists());

		FileStateHandle handle = new FileStateHandle(Path.fromLocalFile(file), file.length());
		handle.discardState();
		assertFalse(file.exists());
	}

	/**
	 * Tests that the file state handle does not attempt to check and clean up the parent directory.
	 * Doing directory contents checks and cleaning up the parent directory in the state handle disposal
	 * has previously led to excessive file system metadata requests, which especially on systems like
	 * Amazon S3 is prohibitively expensive.
	 */
	@Test
	public void testDisposeDoesNotDeleteParentDirectory() throws Exception {
		File parentDir = tempFolder.newFolder();
		assertTrue(parentDir.exists());

		File file = new File(parentDir, "test");
		writeTestData(file);
		assertTrue(file.exists());

		FileStateHandle handle = new FileStateHandle(Path.fromLocalFile(file), file.length());
		handle.discardState();
		assertFalse(file.exists());
		assertTrue(parentDir.exists());
	}

	private static void writeTestData(File file) throws IOException {
		final Random rnd = new Random();

		byte[] data = new byte[rnd.nextInt(1024) + 1];
		rnd.nextBytes(data);

		try (OutputStream out = new FileOutputStream(file)) {
			out.write(data);
		}
	}
}
