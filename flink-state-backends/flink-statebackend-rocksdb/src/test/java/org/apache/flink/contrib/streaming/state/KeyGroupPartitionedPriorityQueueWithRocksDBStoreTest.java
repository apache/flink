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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;

import org.junit.Rule;

/**
 * Test of {@link KeyGroupPartitionedPriorityQueue} powered by a {@link
 * RocksDBCachingPriorityQueueSet}.
 */
public class KeyGroupPartitionedPriorityQueueWithRocksDBStoreTest
        extends InternalPriorityQueueTestBase {

    @Rule public final RocksDBResource rocksDBResource = new RocksDBResource();

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

        return (keyGroupId, numKeyGroups, keyExtractorFunction, elementComparator) -> {
            DataOutputSerializer outputStreamWithPos = new DataOutputSerializer(128);
            DataInputDeserializer inputStreamWithPos = new DataInputDeserializer();
            int keyGroupPrefixBytes =
                    RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(numKeyGroups);
            TreeOrderedSetCache orderedSetCache = new TreeOrderedSetCache(32);
            return new RocksDBCachingPriorityQueueSet<>(
                    keyGroupId,
                    keyGroupPrefixBytes,
                    rocksDBResource.getRocksDB(),
                    rocksDBResource.getReadOptions(),
                    rocksDBResource.getDefaultColumnFamily(),
                    TestElementSerializer.INSTANCE,
                    outputStreamWithPos,
                    inputStreamWithPos,
                    rocksDBResource.getBatchWrapper(),
                    orderedSetCache);
        };
    }
}
