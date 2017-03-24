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

import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.util.FileUtils;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;
import sun.misc.Unsafe;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ListViaRangeSpeedMiniBenchmark {
	
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

		final byte[] keyTemplate = Arrays.copyOf(keyBytes, keyBytes.length + 4);

		final Unsafe unsafe = MemoryUtils.UNSAFE;
		final long offset = unsafe.arrayBaseOffset(byte[].class) + keyTemplate.length - 4;

		final int num = 50000;
		System.out.println("begin insert");

		final long beginInsert = System.nanoTime();
		for (int i = 0; i < num; i++) {
			unsafe.putInt(keyTemplate, offset, i);
			rocksDB.put(write_options, keyTemplate, valueBytes);
		}
		final long endInsert = System.nanoTime();
		System.out.println("end insert - duration: " + ((endInsert - beginInsert) / 1_000_000) + " ms");

		final byte[] resultHolder = new byte[num * valueBytes.length];

		final long beginGet = System.nanoTime();

		final RocksIterator iterator = rocksDB.newIterator();
		int pos = 0;

		// seek to start
		unsafe.putInt(keyTemplate, offset, 0);
		iterator.seek(keyTemplate);

		// mark end
		unsafe.putInt(keyTemplate, offset, -1);

		// iterate
		while (iterator.isValid()) {
			byte[] currKey = iterator.key();
			if (samePrefix(keyBytes, currKey)) {
				byte[] currValue = iterator.value();
				System.arraycopy(currValue, 0, resultHolder, pos, currValue.length);
				pos += currValue.length;
				iterator.next();
			}
			else {
				break;
			}
		}

		final long endGet = System.nanoTime();

		System.out.println("end get - duration: " + ((endGet - beginGet) / 1_000_000) + " ms");
	}

	private static boolean samePrefix(byte[] prefix, byte[] key) {
		for (int i = 0; i < prefix.length; i++) {
			if (prefix[i] != key [i]) {
				return false;
			}
		}

		return true;
	}
}
