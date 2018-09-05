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

package org.apache.flink.contrib.streaming.state.benchmark;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.CompactionStyle;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Test that validates that the performance of RocksDB is as expected.
 * This test guards against the bug filed as 'FLINK-5756'
 */
public class RocksDBPerformanceTest extends TestLogger {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final RetryRule retry = new RetryRule();

	private File rocksDir;
	private Options options;
	private WriteOptions writeOptions;

	private final String key = "key";
	private final String value = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ7890654321";

	private final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
	private final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

	@Before
	public void init() throws IOException {
		rocksDir = tmp.newFolder();

		// ensure the RocksDB library is loaded to a distinct location each retry
		NativeLibraryLoader.getInstance().loadLibrary(rocksDir.getAbsolutePath());

		options = new Options()
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevelCompactionDynamicLevelBytes(true)
				.setIncreaseParallelism(4)
				.setUseFsync(false)
				.setMaxOpenFiles(-1)
				.setCreateIfMissing(true)
				.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);

		writeOptions = new WriteOptions()
				.setSync(false)
				.setDisableWAL(true);
	}

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void testRocksDbMergePerformance() throws Exception {
		final int num = 50000;

		try (RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath())) {

			// ----- insert -----
			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				rocksDB.merge(writeOptions, keyBytes, valueBytes);
			}
			final long endInsert = System.nanoTime();
			log.info("end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);

			// ----- read (attempt 1) -----

			final byte[] resultHolder = new byte[num * (valueBytes.length + 2)];
			final long beginGet1 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet1 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet1 - beginGet1) / 1_000_000);

			// ----- read (attempt 2) -----

			final long beginGet2 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet2 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet2 - beginGet2) / 1_000_000);

			// ----- compact -----
			log.info("compacting...");
			final long beginCompact = System.nanoTime();
			rocksDB.compactRange();
			final long endCompact = System.nanoTime();

			log.info("end compaction - duration: {} ms", (endCompact - beginCompact) / 1_000_000);

			// ----- read (attempt 3) -----

			final long beginGet3 = System.nanoTime();
			rocksDB.get(keyBytes, resultHolder);
			final long endGet3 = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet3 - beginGet3) / 1_000_000);
		}
	}

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void testRocksDbRangeGetPerformance() throws Exception {
		final int num = 50000;

		try (RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath())) {

			final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

			final Unsafe unsafe = MemoryUtils.UNSAFE;
			final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

			log.info("begin insert");

			final long beginInsert = System.nanoTime();
			for (int i = 0; i < num; i++) {
				unsafe.putInt(keyTemplate, offset, i);
				rocksDB.put(writeOptions, keyTemplate, valueBytes);
			}
			final long endInsert = System.nanoTime();
			log.info("end insert - duration: {} ms", (endInsert - beginInsert) / 1_000_000);

			@SuppressWarnings("MismatchedReadAndWriteOfArray")
			final byte[] resultHolder = new byte[num * valueBytes.length];

			final long beginGet = System.nanoTime();

			int pos = 0;

			try (final RocksIteratorWrapper iterator = RocksDBKeyedStateBackend.getRocksIterator(rocksDB)) {
				// seek to start
				unsafe.putInt(keyTemplate, offset, 0);
				iterator.seek(keyTemplate);

				// iterate
				while (iterator.isValid() && samePrefix(keyBytes, iterator.key())) {
					byte[] currValue = iterator.value();
					System.arraycopy(currValue, 0, resultHolder, pos, currValue.length);
					pos += currValue.length;
					iterator.next();
				}
			}

			final long endGet = System.nanoTime();

			log.info("end get - duration: {} ms", (endGet - beginGet) / 1_000_000);
		}
	}

	private static boolean samePrefix(byte[] prefix, byte[] key) {
		for (int i = 0; i < prefix.length; i++) {
			if (prefix[i] != key [i]) {
				return false;
			}
		}

		return true;
	}

	@After
	public void close() {
		options.close();
		writeOptions.close();
	}
}
