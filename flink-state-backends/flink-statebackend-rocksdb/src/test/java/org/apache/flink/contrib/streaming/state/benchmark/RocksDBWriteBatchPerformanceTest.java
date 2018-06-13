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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Test that validates that the performance of RocksDB's WriteBatch as expected.
 *
 * <p>Benchmarking:
 * Computer: MacbookPro (Mid 2015), Flash Storage, Processor 2.5GHz Intel Core i5, Memory 16GB 1600MHz DDR3
 *
 * <p>With disableWAL is false
 * Number of values added | time for Put		|  time for WriteBach | performance improvement of WriteBatch over Put
 * 1000						10146397 ns			3546287 ns				2.86x
 * 10000					118227077 ns		26040222 ns				4.54x
 * 100000					1838593196 ns		375053755 ns			4.9x
 * 1000000					8844612079 ns		2014077396 ns			4.39x
 *
 * <p>With disableWAL is true
 * 1000						3955204 ns			2429725 ns				1.62x
 * 10000					25618237 ns			16440113 ns				1.55x
 * 100000					289153346 ns		183712685 ns			1.57x
 * 1000000					2886298967 ns		1768688571 ns			1.63x
 *
 * <p>In summary:
 *
 * <p>WriteBatch gives users 2.5x-5x performance improvements when disableWAL is false(This is useful when
 * restoring from savepoint, because we need to set disableWAL=true to avoid segfault bug, see FLINK-8859 for detail).
 *
 * <p>Write gives user 1.5x performance improvements when disableWAL is true, this is useful for batch writing scenario,
 * e.g. RocksDBMapState.putAll(Map) & RocksDBMapState.clear().
 */
public class RocksDBWriteBatchPerformanceTest extends TestLogger {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private static final String KEY_PREFIX = "key";

	private static final String VALUE = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ7890654321";

	@Test(timeout = 2000)
	@RetryOnFailure(times = 3)
	public void benchMark() throws Exception {

		int num = 10000;

		List<Tuple2<byte[], byte[]>> data = new ArrayList<>(num);
		for (int i = 0; i < num; ++i) {
			data.add(new Tuple2<>((KEY_PREFIX + i).getBytes(), VALUE.getBytes()));
		}

		log.info("--------------> put VS WriteBatch with disableWAL=false <--------------");

		long t1 = benchMarkHelper(data, false, WRITETYPE.PUT);
		long t2 = benchMarkHelper(data, false, WRITETYPE.WRITE_BATCH);

		log.info("Single Put with disableWAL is false for {} records costs {}" , num, t1);
		log.info("WriteBatch with disableWAL is false for {} records costs {}" , num, t2);

		log.info("--------------> put VS WriteBatch with disableWAL=true <--------------");

		t1 = benchMarkHelper(data, true, WRITETYPE.PUT);
		t2 = benchMarkHelper(data, true, WRITETYPE.WRITE_BATCH);

		log.info("Single Put with disableWAL is true for {} records costs {}" , num, t1);
		log.info("WriteBatch with disableWAL is true for {} records costs {}" , num, t2);
	}

	private enum WRITETYPE {PUT, WRITE_BATCH}

	private long benchMarkHelper(List<Tuple2<byte[], byte[]>> data, boolean disableWAL, WRITETYPE type) throws Exception {
		final File rocksDir = folder.newFolder();

		// ensure the RocksDB library is loaded to a distinct location each retry
		NativeLibraryLoader.getInstance().loadLibrary(rocksDir.getAbsolutePath());

		switch (type) {
			case PUT:
				try (RocksDB db = RocksDB.open(rocksDir.getAbsolutePath());
					WriteOptions options = new WriteOptions().setDisableWAL(disableWAL);
					ColumnFamilyHandle handle = db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()))) {
					long t1 = System.nanoTime();
					for (Tuple2<byte[], byte[]> item : data) {
						db.put(handle, options, item.f0, item.f1);
					}
					return System.nanoTime() - t1;
				}
			case WRITE_BATCH:
				try (RocksDB db = RocksDB.open(rocksDir.getAbsolutePath());
					WriteOptions options = new WriteOptions().setDisableWAL(disableWAL);
					ColumnFamilyHandle handle = db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
					RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(db, options)) {
					long t1 = System.nanoTime();
					for (Tuple2<byte[], byte[]> item : data) {
						writeBatchWrapper.put(handle, item.f0, item.f1);
					}
					writeBatchWrapper.flush();
					return System.nanoTime() - t1;
				}
			default:
				throw new RuntimeException("Unknown benchmark type:" + type);
		}
	}
}
