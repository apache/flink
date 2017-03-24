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

import org.apache.flink.util.FileUtils;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class ListViaMergeSpeedMiniBenchmark {
	
	public static void main(String[] args) throws Exception {
		final File rocksDir = new File("/tmp/rdb");
		FileUtils.deleteDirectory(rocksDir);

		final Options options = new Options()
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevelCompactionDynamicLevelBytes(true)
				.setIncreaseParallelism(4)
				.setUseFsync(false)
				.setMaxOpenFiles(-1)
				.setDisableDataSync(true)
				.setCreateIfMissing(true)
				.setMergeOperator(new StringAppendOperator());

		final WriteOptions write_options = new WriteOptions()
				.setSync(false)
				.setDisableWAL(true);

		final RocksDB rocksDB = RocksDB.open(options, rocksDir.getAbsolutePath());

		final String key = "key";
		final String value = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ7890654321";

		final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

		final int num = 50000;

		// ----- insert -----
		System.out.println("begin insert");

		final long beginInsert = System.nanoTime();
		for (int i = 0; i < num; i++) {
			rocksDB.merge(write_options, keyBytes, valueBytes);
		}
		final long endInsert = System.nanoTime();
		System.out.println("end insert - duration: " + ((endInsert - beginInsert) / 1_000_000) + " ms");

		// ----- read (attempt 1) -----

		final byte[] resultHolder = new byte[num * (valueBytes.length + 2)];
		final long beginGet1 = System.nanoTime();
		rocksDB.get(keyBytes, resultHolder);
		final long endGet1 = System.nanoTime();

		System.out.println("end get - duration: " + ((endGet1 - beginGet1) / 1_000_000) + " ms");

		// ----- read (attempt 2) -----

		final long beginGet2 = System.nanoTime();
		rocksDB.get(keyBytes, resultHolder);
		final long endGet2 = System.nanoTime();

		System.out.println("end get - duration: " + ((endGet2 - beginGet2) / 1_000_000) + " ms");

		// ----- compact -----
		System.out.println("compacting...");
		final long beginCompact = System.nanoTime();
		rocksDB.compactRange();
		final long endCompact = System.nanoTime();

		System.out.println("end compaction - duration: " + ((endCompact - beginCompact) / 1_000_000) + " ms");

		// ----- read (attempt 3) -----

		final long beginGet3 = System.nanoTime();
		rocksDB.get(keyBytes, resultHolder);
		final long endGet3 = System.nanoTime();

		System.out.println("end get - duration: " + ((endGet3 - beginGet3) / 1_000_000) + " ms");
	}
}
