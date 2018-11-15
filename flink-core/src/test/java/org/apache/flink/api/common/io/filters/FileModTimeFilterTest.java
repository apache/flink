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

package org.apache.flink.api.common.io.filters;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the FileModTimeFilter.
 */
public class FileModTimeFilterTest {
	private static final long TEST_TIMESTAMP = 10L;

	private static final FileStatus MOCKED_DIR = new MockedDirFileStatus();
	private static final FileStatus MOCKED_FILE = new MockedFileStatus();

	@Test
	public void testAlwaysAcceptDir() {
		assertTrue(new FileModTimeFilter(TEST_TIMESTAMP - 1).accept(MOCKED_DIR));
		assertTrue(new FileModTimeFilter(TEST_TIMESTAMP).accept(MOCKED_DIR));
		assertTrue(new FileModTimeFilter(TEST_TIMESTAMP + 1).accept(MOCKED_DIR));
	}

	@Test
	public void testFilterFile() {
		assertTrue(new FileModTimeFilter(TEST_TIMESTAMP - 1).accept(MOCKED_FILE));
		assertTrue(new FileModTimeFilter(TEST_TIMESTAMP).accept(MOCKED_FILE));
		assertFalse(new FileModTimeFilter(TEST_TIMESTAMP + 1).accept(MOCKED_FILE));
	}

	private static class MockedFileStatus implements FileStatus {

		@Override
		public long getLen() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getBlockSize() {
			throw new UnsupportedOperationException();
		}

		@Override
		public short getReplication() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getModificationTime() {
			return 10;
		}

		@Override
		public long getAccessTime() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isDir() {
			return false;
		}

		@Override
		public Path getPath() {
			throw new UnsupportedOperationException();
		}
	}

	private static class MockedDirFileStatus implements FileStatus {

		@Override
		public long getLen() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getBlockSize() {
			throw new UnsupportedOperationException();
		}

		@Override
		public short getReplication() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getModificationTime() {
			return 20;
		}

		@Override
		public long getAccessTime() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isDir() {
			return true;
		}

		@Override
		public Path getPath() {
			throw new UnsupportedOperationException();
		}
	}
}
