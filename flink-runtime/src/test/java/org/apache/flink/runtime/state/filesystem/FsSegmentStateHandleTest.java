/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link FsSegmentStateHandle}.
 */
public class FsSegmentStateHandleTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDisposeDeletesFile() throws Exception {
		File file = temporaryFolder.newFile();
		writeTestData(file);
		assertTrue(file.exists());

		FsSegmentStateHandle handle = new FsSegmentStateHandle(Path.fromLocalFile(file), 0, file.length());
		handle.discardState();
		// Discard handle does not delete the underlying file.
		assertTrue(file.exists());
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

