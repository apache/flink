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
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.CompactionStyle;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Test that validates that the performance of APIs of RocksDB's ListState is as expected.
 *
 * <p>Benchmarking:
 *
 * <p>Computer: MacbookPro (Mid 2015), Flash Storage, Processor 2.5GHz Intel Core i7, Memory 16GB 1600MHz DDR3
 * Number of values added | time for add()   |  time for update() | perf improvement of update() over add()
 * 10						236875 ns			17048 ns			13.90x
 * 50						312332 ns			14281 ns			21.87x
 * 100						393791 ns			18360 ns			21.45x
 * 500						978703 ns			55397 ns			17.66x
 * 1000						3044179 ns			89474 ns			34.02x
 * 5000						9247395 ns			305580 ns			30.26x
 * 10000					16416442 ns			605963 ns			27.09x
 * 50000					84311205 ns			5691288 ns			14.81x
 * 100000					195103310 ns		12914182 ns			15.11x
 * 500000					1223141510 ns		70595881 ns			17.33x
 *
 * <p>In summary, update() API which pre-merges all values gives users 15-35x performance improvements.
 * For most frequent use cases where there are a few hundreds to a few thousands values per key,
 * users can get 30x - 35x performance improvement!
 *
 */
public class RocksDBListStatePerformanceTest extends TestLogger {

	private static final byte DELIMITER = ',';

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final RetryRule retry = new RetryRule();

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void testRocksDbListStateAPIs() throws Exception {
		final File rocksDir = tmp.newFolder();

		// ensure the RocksDB library is loaded to a distinct location each retry
		NativeLibraryLoader.getInstance().loadLibrary(rocksDir.getAbsolutePath());

		final String key1 = "key1";
		final String key2 = "key2";
		final String value = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ7890654321";

		final byte[] keyBytes1 = key1.getBytes(StandardCharsets.UTF_8);
		final byte[] keyBytes2 = key2.getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

		// The number of values added to ListState. Can be changed for benchmarking
		final int num = 10;

		try (
			final Options options = new Options()
					.setCompactionStyle(CompactionStyle.LEVEL)
					.setLevelCompactionDynamicLevelBytes(true)
					.setIncreaseParallelism(4)
					.setUseFsync(false)
					.setMaxOpenFiles(-1)
					.setCreateIfMissing(true)
					.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);

			final WriteOptions writeOptions = new WriteOptions()
					.setSync(false)
					.setDisableWAL(true);

			final RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath())) {

			// ----- add() API -----
			log.info("begin add");

			final long beginInsert1 = System.nanoTime();
			for (int i = 0; i < num; i++) {
				rocksDB.merge(writeOptions, keyBytes1, valueBytes);
			}
			final long endInsert1 = System.nanoTime();

			log.info("end add - duration: {} ns", (endInsert1 - beginInsert1));

			// ----- update() API -----

			List<byte[]> list = new ArrayList<>(num);
			for (int i = 0; i < num; i++) {
				list.add(valueBytes);
			}
			byte[] premerged = merge(list);

			log.info("begin update");

			final long beginInsert2 = System.nanoTime();
			rocksDB.merge(writeOptions, keyBytes2, premerged);
			final long endInsert2 = System.nanoTime();

			log.info("end update - duration: {} ns", (endInsert2 - beginInsert2));
		}
	}

	/**
	 * Merge operands into a single value that can be put directly into RocksDB.
	 */
	public static byte[] merge(List<byte[]> operands) {
		if (operands == null || operands.size() == 0) {
			return null;
		}

		if (operands.size() == 1) {
			return operands.get(0);
		}

		int numBytes = 0;
		for (byte[] arr : operands) {
			numBytes += arr.length + 1;
		}
		numBytes--;

		byte[] result = new byte[numBytes];

		System.arraycopy(operands.get(0), 0, result, 0, operands.get(0).length);

		for (int i = 1, arrIndex = operands.get(0).length; i < operands.size(); i++) {
			result[arrIndex] = DELIMITER;
			arrIndex += 1;
			System.arraycopy(operands.get(i), 0, result, arrIndex, operands.get(i).length);
			arrIndex += operands.get(i).length;
		}

		return result;
	}
}
