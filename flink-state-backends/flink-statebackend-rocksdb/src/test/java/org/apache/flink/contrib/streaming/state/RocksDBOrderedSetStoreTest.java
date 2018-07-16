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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.util.CloseableIterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.NoSuchElementException;

/**
 * Test for RocksDBOrderedStore.
 */
public class RocksDBOrderedSetStoreTest {

	@Rule
	public final RocksDBResource rocksDBResource = new RocksDBResource();

	@Test
	public void testOrderedIterator() throws Exception {
		CachingInternalPriorityQueueSet.OrderedSetStore<Integer> store = createRocksDBOrderedStore();

		//test empty iterator
		try (final CloseableIterator<Integer> emptyIterator = store.orderedIterator()) {
			Assert.assertFalse(emptyIterator.hasNext());
			try {
				emptyIterator.next();
				Assert.fail();
			} catch (NoSuchElementException expected) {
			}
		}

		store.add(43);
		store.add(42);
		store.add(41);
		store.add(41);
		store.remove(42);

		// test in-order iteration
		try (final CloseableIterator<Integer> iterator = store.orderedIterator()) {
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(Integer.valueOf(41), iterator.next());
			Assert.assertTrue(iterator.hasNext());
			Assert.assertEquals(Integer.valueOf(43), iterator.next());
			Assert.assertFalse(iterator.hasNext());
			try {
				iterator.next();
				Assert.fail();
			} catch (NoSuchElementException expected) {
			}
		}
	}

	@Test
	public void testAddRemoveSize() {

		CachingInternalPriorityQueueSet.OrderedSetStore<Integer> store = createRocksDBOrderedStore();

		// test empty size
		Assert.assertEquals(0, store.size());

		// test add uniques
		store.remove(41);
		Assert.assertEquals(0, store.size());
		store.add(41);
		Assert.assertEquals(1, store.size());
		store.add(42);
		Assert.assertEquals(2, store.size());
		store.add(43);
		Assert.assertEquals(3, store.size());
		store.add(44);
		Assert.assertEquals(4, store.size());
		store.add(45);
		Assert.assertEquals(5, store.size());

		// test remove
		store.remove(41);
		Assert.assertEquals(4, store.size());
		store.remove(41);
		Assert.assertEquals(4, store.size());

		// test set semantics by attempt to insert duplicate
		store.add(42);
		Assert.assertEquals(4, store.size());
	}

	public static <E> RocksDBOrderedSetStore<E> createRocksDBOrderedStore(
		@Nonnull RocksDBResource rocksDBResource,
		@Nonnull TypeSerializer<E> byteOrderSerializer,
		@Nonnegative int keyGroupId,
		@Nonnegative int totalKeyGroups) {

		ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);
		int keyGroupPrefixBytes = RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
		return new RocksDBOrderedSetStore<>(
			keyGroupId,
			keyGroupPrefixBytes,
			rocksDBResource.getRocksDB(),
			rocksDBResource.getDefaultColumnFamily(),
			byteOrderSerializer,
			outputStreamWithPos,
			outputView,
			rocksDBResource.getBatchWrapper());
	}

	protected RocksDBOrderedSetStore<Integer> createRocksDBOrderedStore() {
		return createRocksDBOrderedStore(rocksDBResource, IntSerializer.INSTANCE, 0, 1);
	}
}
