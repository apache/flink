/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests to guard {@link RocksDBIncrementalCheckpointUtils}.
 */
public class RocksDBIncrementalCheckpointUtilsTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testClipDBWithKeyGroupRange() throws Exception {

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(0, 1), new KeyGroupRange(0, 2), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(0, 1), new KeyGroupRange(0, 1), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(0, 1), new KeyGroupRange(1, 2), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(0, 1), new KeyGroupRange(2, 4), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(Byte.MAX_VALUE - 15, Byte.MAX_VALUE), new KeyGroupRange(Byte.MAX_VALUE - 10, Byte.MAX_VALUE), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(Short.MAX_VALUE - 15, Short.MAX_VALUE), new KeyGroupRange(Short.MAX_VALUE - 10, Short.MAX_VALUE), 2);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(Byte.MAX_VALUE - 15, Byte.MAX_VALUE - 1), new KeyGroupRange(Byte.MAX_VALUE - 10, Byte.MAX_VALUE), 1);

		testClipDBWithKeyGroupRangeHelper(new KeyGroupRange(Short.MAX_VALUE - 15, Short.MAX_VALUE - 1), new KeyGroupRange(Short.MAX_VALUE - 10, Short.MAX_VALUE), 2);
	}

	void testClipDBWithKeyGroupRangeHelper(
		KeyGroupRange targetGroupRange,
		KeyGroupRange currentGroupRange,
		int keyGroupPrefixBytes) throws RocksDBException, IOException {

		RocksDB rocksDB = RocksDB.open(tmp.newFolder().getAbsolutePath());
		ColumnFamilyHandle columnFamilyHandle = null;
		try {

			columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));

			int currentGroupRangeStart = currentGroupRange.getStartKeyGroup();
			int currentGroupRangeEnd = currentGroupRange.getEndKeyGroup();

			for (int i = currentGroupRangeStart; i <= currentGroupRangeEnd; ++i) {
				ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
				DataOutputView outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);
				for (int j = 0; j < 100; ++j) {
					outputStreamWithPos.reset();
					RocksDBKeySerializationUtils.writeKeyGroup(i, keyGroupPrefixBytes, outputView);
					RocksDBKeySerializationUtils.writeKey(
						j,
						IntSerializer.INSTANCE,
						outputStreamWithPos,
						new DataOutputViewStreamWrapper(outputStreamWithPos),
						false);
					rocksDB.put(columnFamilyHandle, outputStreamWithPos.toByteArray(), String.valueOf(j).getBytes());
				}
			}

			for (int i = currentGroupRangeStart; i <= currentGroupRangeEnd; ++i) {
				ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
				DataOutputView outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);
				for (int j = 0; j < 100; ++j) {
					outputStreamWithPos.reset();
					RocksDBKeySerializationUtils.writeKeyGroup(i, keyGroupPrefixBytes, outputView);
					RocksDBKeySerializationUtils.writeKey(
						j,
						IntSerializer.INSTANCE,
						outputStreamWithPos,
						new DataOutputViewStreamWrapper(outputStreamWithPos),
						false);
					byte[] value = rocksDB.get(columnFamilyHandle, outputStreamWithPos.toByteArray());
					Assert.assertEquals(String.valueOf(j), new String(value));
				}
			}

			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				rocksDB,
				Collections.singletonList(columnFamilyHandle),
				targetGroupRange,
				currentGroupRange,
				keyGroupPrefixBytes);

			for (int i = currentGroupRangeStart; i <= currentGroupRangeEnd; ++i) {
				ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
				DataOutputView outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);
				for (int j = 0; j < 100; ++j) {
					outputStreamWithPos.reset();
					RocksDBKeySerializationUtils.writeKeyGroup(i, keyGroupPrefixBytes, outputView);
					RocksDBKeySerializationUtils.writeKey(
						j,
						IntSerializer.INSTANCE,
						outputStreamWithPos,
						new DataOutputViewStreamWrapper(outputStreamWithPos),
						false);
					byte[] value = rocksDB.get(
						columnFamilyHandle, outputStreamWithPos.toByteArray());
					if (targetGroupRange.contains(i)) {
						Assert.assertEquals(String.valueOf(j), new String(value));
					} else {
						Assert.assertNull(value);
					}
				}
			}
		} finally {
			if (columnFamilyHandle != null)	 {
				columnFamilyHandle.close();
			}

			if (rocksDB != null) {
				rocksDB.close();
			}
		}
	}

	@Test
	public void testEvaluateGroupRange() throws Exception {
		KeyGroupRange range1 = new KeyGroupRange(0, 9);
		KeyGroupRange range2 = new KeyGroupRange(0, 9);

		Assert.assertEquals(10, RocksDBIncrementalCheckpointUtils.evaluateGroupRange(range1, range2));

		KeyGroupRange range3 = new KeyGroupRange(0, 9);
		KeyGroupRange range4 = new KeyGroupRange(9, 10);

		Assert.assertEquals(1, RocksDBIncrementalCheckpointUtils.evaluateGroupRange(range3, range4));

		KeyGroupRange range5 = new KeyGroupRange(0, 9);
		KeyGroupRange range6 = new KeyGroupRange(10, 12);

		Assert.assertEquals(0, RocksDBIncrementalCheckpointUtils.evaluateGroupRange(range5, range6));

		KeyGroupRange range7 = new KeyGroupRange(0, 9);
		KeyGroupRange range8 = new KeyGroupRange(1, 2);

		Assert.assertEquals(2, RocksDBIncrementalCheckpointUtils.evaluateGroupRange(range7, range8));
	}
}
