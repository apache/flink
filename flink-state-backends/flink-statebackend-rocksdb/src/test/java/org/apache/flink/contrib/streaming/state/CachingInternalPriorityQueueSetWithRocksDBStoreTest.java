/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSetTestBase;
import org.apache.flink.runtime.state.heap.TreeOrderedSetCache;

import org.junit.Rule;

/**
 * Test for {@link CachingInternalPriorityQueueSet} with a
 * {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetStore} based on RocksDB.
 */
public class CachingInternalPriorityQueueSetWithRocksDBStoreTest extends CachingInternalPriorityQueueSetTestBase {

	@Rule
	public final RocksDBResource rocksDBResource = new RocksDBResource();

	@Override
	protected CachingInternalPriorityQueueSet.OrderedSetStore<TestElement> createOrderedSetStore() {
		return createRocksDBStore(0, 1, rocksDBResource);
	}

	@Override
	protected CachingInternalPriorityQueueSet.OrderedSetCache<TestElement> createOrderedSetCache() {
		return new TreeOrderedSetCache<>(TEST_ELEMENT_COMPARATOR, 64);
	}

	public static CachingInternalPriorityQueueSet.OrderedSetStore<TestElement> createRocksDBStore(
		int keyGroupId,
		int totalKeyGroups,
		RocksDBResource rocksDBResource) {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos(16);
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
		int prefixBytes = RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
		return new RocksDBOrderedSetStore<>(
			keyGroupId,
			prefixBytes,
			rocksDBResource.getRocksDB(),
			rocksDBResource.getDefaultColumnFamily(),
			rocksDBResource.getReadOptions(),
			TestElementSerializer.INSTANCE,
			outputStream,
			outputView,
			rocksDBResource.getBatchWrapper());
	}
}
