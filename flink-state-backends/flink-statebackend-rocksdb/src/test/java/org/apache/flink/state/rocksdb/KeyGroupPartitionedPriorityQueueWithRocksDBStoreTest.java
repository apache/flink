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

package org.apache.flink.state.rocksdb;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.FlushOptions;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test of {@link KeyGroupPartitionedPriorityQueue} powered by a {@link
 * RocksDBCachingPriorityQueueSet}.
 */
class KeyGroupPartitionedPriorityQueueWithRocksDBStoreTest extends InternalPriorityQueueTestBase {

    @RegisterExtension public final RocksDBExtension rocksDBExtension = new RocksDBExtension();

    @Test
    void testQueueInitializationDoesNotScanTombstonesOutsideKeyGroupRange() throws Exception {
        // Deleted entries of the key group right after the range, as left behind by rescaling.
        createTombstonesInKeyGroup(128, 512);
        rocksDBExtension.getReadOptions().setMaxSkippableInternalKeys(8);
        final InternalPriorityQueue<TestElement> queue =
                new KeyGroupPartitionedPriorityQueue<>(
                        KEY_EXTRACTOR_FUNCTION,
                        TEST_ELEMENT_PRIORITY_COMPARATOR,
                        newFactory(),
                        new KeyGroupRange(0, 127),
                        512);

        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void testCacheRefillKeepsBoundsOfKeyGroup() throws Exception {
        final RocksDBCachingPriorityQueueSet<TestElement> queue =
                newPriorityQueueForKeyGroup(255, 512, 1);
        final TestElement first = new TestElement(1, 1);
        final TestElement second = new TestElement(2, 2);
        queue.add(first);
        queue.add(second);
        assertThat(queue.poll()).isEqualTo(first);
        createTombstonesInKeyGroup(256, 512);

        rocksDBExtension.getReadOptions().setMaxSkippableInternalKeys(8);
        assertThat(queue.poll()).isEqualTo(second);
        assertThat(queue.poll()).isNull();
    }

    private void createTombstonesInKeyGroup(int keyGroupId, int numKeyGroups) throws Exception {
        final RocksDB db = rocksDBExtension.getRocksDB();
        db.setOptions(
                rocksDBExtension.getDefaultColumnFamily(),
                MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true).build());

        final byte[][] keys = new byte[32][];
        final DataOutputSerializer output = new DataOutputSerializer(32);
        for (int i = 0; i < keys.length; i++) {
            output.clear();
            CompositeKeySerializationUtils.writeKeyGroup(
                    keyGroupId,
                    CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                            numKeyGroups),
                    output);
            TestElementSerializer.INSTANCE.serialize(new TestElement(i, i), output);
            keys[i] = output.getCopyOfBuffer();
            db.put(rocksDBExtension.getDefaultColumnFamily(), keys[i], new byte[0]);
        }

        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            rocksDBExtension.getBatchWrapper().flush();
            db.flush(flushOptions);
            for (byte[] key : keys) {
                db.delete(rocksDBExtension.getDefaultColumnFamily(), key);
            }
            db.flush(flushOptions);
        }

        assertThat(
                        db.getPropertiesOfAllTables().values().stream()
                                .mapToLong(TableProperties::getNumDeletions)
                                .sum())
                .isGreaterThanOrEqualTo(keys.length);
    }

    @Override
    protected InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
        return new KeyGroupPartitionedPriorityQueue<>(
                KEY_EXTRACTOR_FUNCTION,
                TEST_ELEMENT_PRIORITY_COMPARATOR,
                newFactory(),
                KEY_GROUP_RANGE,
                KEY_GROUP_RANGE.getNumberOfKeyGroups());
    }

    @Override
    protected boolean testSetSemanticsAgainstDuplicateElements() {
        return true;
    }

    private KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<
                    TestElement, RocksDBCachingPriorityQueueSet<TestElement>>
            newFactory() {

        return (keyGroupId, numKeyGroups, keyExtractorFunction, elementComparator) ->
                newPriorityQueueForKeyGroup(keyGroupId, numKeyGroups, 32);
    }

    private RocksDBCachingPriorityQueueSet<TestElement> newPriorityQueueForKeyGroup(
            int keyGroupId, int numKeyGroups, int cacheSize) {
        final int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(numKeyGroups);
        return new RocksDBCachingPriorityQueueSet<>(
                keyGroupId,
                keyGroupPrefixBytes,
                rocksDBExtension.getRocksDB(),
                rocksDBExtension.getReadOptions(),
                rocksDBExtension.getDefaultColumnFamily(),
                TestElementSerializer.INSTANCE,
                new DataOutputSerializer(128),
                new DataInputDeserializer(),
                rocksDBExtension.getBatchWrapper(),
                new TreeOrderedSetCache(cacheSize));
    }
}
