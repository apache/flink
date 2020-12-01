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

import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the {@link FSDataBufferedInputStream}.
 */
@RunWith(Parameterized.class)
public class FSDataBufferedInputStreamTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameterized.Parameters
	public static List<Integer> getDataSize() {
		return Arrays.asList(10, 100, 1000, 10000, 50000);
	}

	@Parameterized.Parameter
	public int dataSize;

	LocalFileSystem lfs;
	Path pathToTestFile;

	@Before
	public void before() throws IOException {
		// prepare file
		final File tempDir = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());

		final File testFile = new File(tempDir, UUID.randomUUID().toString());
		pathToTestFile = new Path(testFile.toURI().getPath());

		lfs = new LocalFileSystem();
		final FSDataOutputStream lfsOutput = lfs.create(
			pathToTestFile,
			FileSystem.WriteMode.NO_OVERWRITE);
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(
			lfsOutput);

		for (int i = 0; i < dataSize; i++) {
			outputView.writeInt(i);
		}
		lfsOutput.close();

		assertEquals(testFile.length(), Integer.BYTES * dataSize);

	}

	@Test
	public void testOrderRead() throws Exception {
		FSDataInputStream lfsInput = new FSDataBufferedInputStream(lfs.open(pathToTestFile));
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(lfsInput);
		for (int i = 0; i < dataSize; i++) {
			int data = inputView.readInt();
			assertEquals(i, data);
		}
		lfsInput.close();
	}

	@Test
	public void testSeekRead() throws Exception {
		FSDataInputStream lfsInput = new FSDataBufferedInputStream(lfs.open(pathToTestFile));
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(lfsInput);

		// test for descending read
		int expectedStateSize = dataSize * Integer.BYTES;
		for (int i = dataSize - 1; i >= 0; i--) {
			int offset = Integer.BYTES * i;
			// test for pos and available
			lfsInput.seek(offset);
			Assert.assertEquals(offset, lfsInput.getPos());
			int avail = expectedStateSize - offset;
			Assert.assertEquals(avail, lfsInput.available());

			// test for data
			int read = inputView.readInt();
			Assert.assertEquals(i, read);
		}

		// test for random read
		Random random = new Random();
		for (int j = 0; j < dataSize * 2; j++) {
			int index = random.nextInt(dataSize);

			int offset = Integer.BYTES * index;
			// test for pos and available
			lfsInput.seek(offset);
			Assert.assertEquals(offset, lfsInput.getPos());
			int avail = expectedStateSize - offset;
			Assert.assertEquals(avail, lfsInput.available());

			// test for data
			int read = inputView.readInt();
			Assert.assertEquals(index, read);
		}

		lfsInput.close();
	}

}
