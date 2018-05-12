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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;

/**
 * Tests to guard {@link TwoPhraseFSDataOutputStream}.
 */
public class TwoPhraseFSDataOutputStreamTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testWriteSuccessfully() throws Exception {

		final String tmpPath = temporaryFolder.newFolder("testTwoPhraseFSDataOutputStream").getPath();
		final Path targetFile = new Path(tmpPath, "target");
		final FileSystem fileSystem = targetFile.getFileSystem();

		byte[] expectedData = new byte[512];
		new Random().nextBytes(expectedData);

		testWriteData(false, expectedData, targetFile);

		try {
			testWriteData(false, expectedData, targetFile);
			Assert.fail("Should fail because the target file is already exists, and we are using NO_OVERWRITE model");
		} catch (Exception expected) {
			// we expected it.
		}

		Assert.assertTrue(fileSystem.exists(targetFile));

		try (FSDataInputStream inputStream = fileSystem.open(targetFile)) {
			byte[] data = new byte[512];
			Assert.assertEquals(512, inputStream.read(data));
			Assert.assertArrayEquals(expectedData, data);
		}
	}

	@Test
	public void testWriteFailed() throws Exception {

		final String tmpPath = temporaryFolder.newFolder("testTwoPhraseFSDataOutputStream").getPath();
		final Path targetFile = new Path(tmpPath, "target");
		final FileSystem fileSystem = targetFile.getFileSystem();

		byte[] expectedData = new byte[512];
		new Random().nextBytes(expectedData);

		try {
			testWriteData(true, expectedData, targetFile);
			Assert.fail("Should fail because we have triggered an exception.");
		} catch (Exception expected) {
			Assert.assertFalse(fileSystem.exists(targetFile));
		}
	}

	private void testWriteData(boolean triggerException, byte[] data, Path targetFile) throws Exception {

		final FileSystem fileSystem = targetFile.getFileSystem();

		try (TwoPhraseFSDataOutputStream outputStream = new TwoPhraseFSDataOutputStream(targetFile.getFileSystem(), targetFile, FileSystem.WriteMode.NO_OVERWRITE)) {

			outputStream.write(data);

			Assert.assertTrue(fileSystem.exists(outputStream.getPreparingFile()));
			Assert.assertFalse(fileSystem.exists(targetFile));

			if (triggerException) {
				throw new Exception("Trigger exception on purpose.");
			}

			outputStream.commit();
		}
	}
}
