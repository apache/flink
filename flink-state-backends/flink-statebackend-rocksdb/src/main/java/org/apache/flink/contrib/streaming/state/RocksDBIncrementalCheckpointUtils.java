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

import org.apache.flink.runtime.state.KeyGroupRange;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;

/**
 * Utils for RocksDB Incremental Checkpoint.
 */
public class RocksDBIncrementalCheckpointUtils {

	public static void clipDBWithKeyGroupRange(
		RocksDB db,
		List<ColumnFamilyHandle> columnFamilyHandles,
		KeyGroupRange targetGroupRange,
		KeyGroupRange currentGroupRange,
		int keyGroupPrefixBytes) throws RocksDBException {

		for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
			if (currentGroupRange.getStartKeyGroup() < targetGroupRange.getStartKeyGroup()) {
				byte[] beginKey = RocksDBKeySerializationUtils.serializeKeyGroup(
					currentGroupRange.getStartKeyGroup(), keyGroupPrefixBytes);
				byte[] endKye = RocksDBKeySerializationUtils.serializeKeyGroup(
					targetGroupRange.getStartKeyGroup(), keyGroupPrefixBytes);
				db.deleteRange(columnFamilyHandle, beginKey, endKye);
			}

			if (currentGroupRange.getEndKeyGroup() > targetGroupRange.getEndKeyGroup()) {
				byte[] beginKey = RocksDBKeySerializationUtils.serializeKeyGroup(
					targetGroupRange.getEndKeyGroup() + 1, keyGroupPrefixBytes);

				byte[] endKey = new byte[keyGroupPrefixBytes];
				for (int i = 0; i < keyGroupPrefixBytes; ++i) {
					endKey[i] = (byte) (0xFF);
				}
				db.deleteRange(columnFamilyHandle, beginKey, endKey);
			}
		}
	}

	public static int evaluateGroupRange(KeyGroupRange range1, KeyGroupRange range2) {
		int start1 = range1.getStartKeyGroup();
		int end1 = range1.getEndKeyGroup();

		int start2 = range2.getStartKeyGroup();
		int end2 = range2.getEndKeyGroup();

		int insectStart = start1;
		if (start2 > start1) {
			insectStart = start2;
		}

		int insectEnd = end1;
		if (end2 < end1) {
			insectEnd = end2;
		}
		return insectEnd >= insectStart ? insectEnd - insectStart + 1 : 0;
	}
}
