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

import java.io.IOException;
import java.util.Random;

/**
 * Tests to guard {@link TwoPhaseFSDataOutputStream}.
 */
public class TwoPhaseFSDataOutputStreamTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testWriteSuccessfully() throws Exception {

		final String tmpPath = temporaryFolder.newFolder("testTwoPhaseFSDataOutputStream").getPath();
		final Path targetFile = new Path(tmpPath, "target");
		final FileSystem fileSystem = targetFile.getFileSystem();

		byte[] expectedData = new byte[102400];
		new Random().nextBytes(expectedData);

		writeData(false, expectedData, targetFile);

		Assert.assertTrue(fileSystem.exists(targetFile));

		byte[] data = new byte[102400];
		readData(data, targetFile);
		Assert.assertArrayEquals(expectedData, data);
	}

	@Test
	public void testWriteMode() throws Exception {

		final String tmpPath = temporaryFolder.newFolder("testTwoPhaseFSDataOutputStream").getPath();
		final Path targetFile = new Path(tmpPath, "target");
		try (TwoPhaseFSDataOutputStream outputStream = new TwoPhaseFSDataOutputStream(targetFile.getFileSystem(), targetFile, FileSystem.WriteMode.NO_OVERWRITE)) {
			outputStream.closeAndPublish();
		} catch (Exception ex) {
			Assert.fail("failed to create TwoPhaseFSDataOutputStream with WriteMode.NO_OVERWRITE.");
		}

		try (TwoPhaseFSDataOutputStream outputStream = new TwoPhaseFSDataOutputStream(targetFile.getFileSystem(), targetFile, FileSystem.WriteMode.NO_OVERWRITE)) {
			Assert.fail("should fail to create TwoPhaseFSDataOutputStream because the target file is already exists.");
		} catch (IOException expected) {
			//expected this exception.
		}

		try (TwoPhaseFSDataOutputStream outputStream = new TwoPhaseFSDataOutputStream(targetFile.getFileSystem(), targetFile, FileSystem.WriteMode.OVERWRITE)) {
			Assert.fail("should fail to create TwoPhaseFSDataOutputStream with WriteMode.OVERWRITE.");
		} catch (IllegalArgumentException expected) {
			//expected this exception.
		}
	}

	@Test
	public void testWriteFailed() throws Exception {

		final String tmpPath = temporaryFolder.newFolder("testTwoPhaseFSDataOutputStream").getPath();
		final Path targetFile = new Path(tmpPath, "target");
		final FileSystem fileSystem = targetFile.getFileSystem();

		byte[] expectedData = new byte[512];
		new Random().nextBytes(expectedData);

		try {
			writeData(true, expectedData, targetFile);
			Assert.fail("Should fail because we have triggered an exception.");
		} catch (Exception expected) {
			Assert.assertFalse(fileSystem.exists(targetFile));
		}
	}

	private void writeData(boolean triggerException, byte[] data, Path targetFile) throws Exception {

		final FileSystem fileSystem = targetFile.getFileSystem();

		try (TwoPhaseFSDataOutputStream outputStream = new TwoPhaseFSDataOutputStream(targetFile.getFileSystem(), targetFile, FileSystem.WriteMode.NO_OVERWRITE)) {

			outputStream.write(data);

			Assert.assertTrue(fileSystem.exists(outputStream.getPreparingFile()));
			Assert.assertFalse(fileSystem.exists(targetFile));

			if (triggerException) {
				throw new Exception("Trigger exception on purpose.");
			}

			outputStream.closeAndPublish();
		}
	}

	private void readData(byte[] data, Path targetFile) throws Exception {
		try (FSDataInputStream inputStream = targetFile.getFileSystem().open(targetFile)) {
			int totalSize = data.length;
			int len;
			int pos = 0;
			while (pos < totalSize && (len = inputStream.read(data, pos, totalSize - pos)) > 0) {
				pos += len;
			}
		}
	}
}
