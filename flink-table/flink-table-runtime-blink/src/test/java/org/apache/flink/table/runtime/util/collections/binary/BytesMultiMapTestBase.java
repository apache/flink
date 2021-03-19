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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.flink.table.runtime.util.collections.binary.BytesHashMapTestBase.VALUE_TYPES;

/** Base test class for binary multi-map. */
public abstract class BytesMultiMapTestBase<K> extends BytesMapTestBase {
    protected static final int NUM_VALUE_PER_KEY = 50;
    protected final RandomKeyGenerator<K> generator;

    protected final PagedTypeSerializer<K> keySerializer;
    protected final BinaryRowDataSerializer valueSerializer;

    public BytesMultiMapTestBase(
            PagedTypeSerializer<K> keySerializer, RandomKeyGenerator<K> generator) {
        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(VALUE_TYPES.length);
        this.generator = generator;
    }

    abstract BinaryRowData[] genValues(int num);

    // ------------------------------------------------------------------------------------------
    // Utils
    // ------------------------------------------------------------------------------------------

    protected void innerBuildAndRetrieve(MapCreator<K> creator, MapValidator<K> validator)
            throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(numMemSegments * PAGE_SIZE).build();

        AbstractBytesMultiMap<K> table = creator.createMap(memoryManager, memorySize);

        K[] keys = generator.createKeys(KEY_TYPES, NUM_ENTRIES / 10);
        BinaryRowData[] values = genValues(NUM_VALUE_PER_KEY);

        for (K key : keys) {
            BytesMap.LookupInfo<K, Iterator<RowData>> lookupInfo;
            for (BinaryRowData value : values) {
                lookupInfo = table.lookup(key);
                table.append(lookupInfo, value);
            }
        }

        KeyValueIterator<K, Iterator<RowData>> iter = table.getEntryIterator();
        validator.validate(keys, values, iter);
    }

    /** Creates the specific multi-map. */
    interface MapCreator<K> {
        AbstractBytesMultiMap<K> createMap(MemoryManager memoryManager, int memorySize);
    }

    interface MapValidator<K> {
        void validate(
                K[] keys, BinaryRowData[] values, KeyValueIterator<K, Iterator<RowData>> actual)
                throws IOException;
    }
}
