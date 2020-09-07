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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.OperatingSystem;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@link RocksDBOperationUtils}.
 */
public class RocksDBOperationsUtilsTest {

	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	@BeforeClass
	public static void loadRocksLibrary() throws Exception {
		NativeLibraryLoader.getInstance().loadLibrary(TMP_DIR.newFolder().getAbsolutePath());
	}

	@Test
	public void testPathExceptionOnWindows() throws Exception {
		assumeTrue(OperatingSystem.isWindows());

		final File folder = TMP_DIR.newFolder();
		final File rocksDir = new File(folder, getLongString(247 - folder.getAbsolutePath().length()));

		Files.createDirectories(rocksDir.toPath());

		try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
			ColumnFamilyOptions colOptions = new ColumnFamilyOptions()) {

			RocksDB rocks = RocksDBOperationUtils.openDB(
					rocksDir.getAbsolutePath(),
					Collections.emptyList(),
					Collections.emptyList(),
					colOptions, dbOptions);
			rocks.close();

			// do not provoke a test failure if this passes, because some setups may actually
			// support long paths, in which case: great!
		}
		catch (IOException e) {
			assertThat(e.getMessage(), containsString("longer than the directory path length limit for Windows"));
		}
	}

	@Test
	public void testSanityCheckArenaBlockSize() {
		List<TestData> tests = Arrays.asList(
			new TestData(67108864, 0, 8388608, false),
			new TestData(67108864, 8388608, 8388608, false),
			new TestData(67108864, 0, 11184810, true),
			new TestData(67108864, 8388608, 11184810, true)
		);

		for (TestData test : tests) {
			long writeBufferSize = test.getWriteBufferSize();
			long arenaBlockSizeConfigured = test.getArenaBlockSizeConfigured();
			long writeBufferManagerCapacity = test.getWriteBufferManagerCapacity();
			boolean expected = test.isExpected();

			boolean isOk = RocksDBOperationUtils.sanityCheckArenaBlockSize(writeBufferSize, arenaBlockSizeConfigured, writeBufferManagerCapacity);
			if (expected) {
				assertTrue(isOk);
			} else {
				assertFalse(isOk);
			}
		}
	}

	private static class TestData {
		private final long writeBufferSize;
		private final long arenaBlockSizeConfigured;
		private final long writeBufferManagerCapacity;
		private final boolean expected;

		public TestData(long writeBufferSize, long arenaBlockSizeConfigured, long writeBufferManagerCapacity, boolean expected) {
			this.writeBufferSize = writeBufferSize;
			this.arenaBlockSizeConfigured = arenaBlockSizeConfigured;
			this.writeBufferManagerCapacity = writeBufferManagerCapacity;
			this.expected = expected;
		}

		public long getWriteBufferSize() {
			return writeBufferSize;
		}

		public long getArenaBlockSizeConfigured() {
			return arenaBlockSizeConfigured;
		}

		public long getWriteBufferManagerCapacity() {
			return writeBufferManagerCapacity;
		}

		public boolean isExpected() {
			return expected;
		}
	}

	private static String getLongString(int numChars) {
		final StringBuilder builder = new StringBuilder();
		for (int i = numChars; i > 0; --i) {
			builder.append('a');
		}
		return builder.toString();
	}
}
