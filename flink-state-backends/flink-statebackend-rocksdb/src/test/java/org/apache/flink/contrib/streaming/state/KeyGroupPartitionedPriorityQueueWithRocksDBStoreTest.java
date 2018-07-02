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

import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueueTest;
import org.apache.flink.runtime.state.heap.TreeOrderedSetCache;

import org.junit.Rule;

/**
 * Test of {@link KeyGroupPartitionedPriorityQueue} powered by a {@link RocksDBOrderedSetStore}.
 */
public class KeyGroupPartitionedPriorityQueueWithRocksDBStoreTest extends KeyGroupPartitionedPriorityQueueTest {

	@Rule
	public final RocksDBResource rocksDBResource = new RocksDBResource();

	@Override
	protected KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<
			TestElement, CachingInternalPriorityQueueSet<TestElement>> newFactory(
		int initialCapacity) {

		return (keyGroupId, numKeyGroups, elementComparator) -> {
			CachingInternalPriorityQueueSet.OrderedSetCache<TestElement> cache =
				new TreeOrderedSetCache<>(TEST_ELEMENT_COMPARATOR, 4);
			CachingInternalPriorityQueueSet.OrderedSetStore<TestElement> store =
				RocksDBOrderedSetStoreTest.createRocksDBOrderedStore(
					rocksDBResource,
					TestElementSerializer.INSTANCE,
					keyGroupId,
					numKeyGroups);
			return new CachingInternalPriorityQueueSet<>(cache, store);
		};
	}
}
