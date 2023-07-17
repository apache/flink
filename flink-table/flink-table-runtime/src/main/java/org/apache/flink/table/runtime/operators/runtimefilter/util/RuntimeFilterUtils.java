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

package org.apache.flink.table.runtime.operators.runtimefilter.util;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/** Utilities for runtime filter. */
public class RuntimeFilterUtils {

    public static final int OVER_MAX_ROW_COUNT = -1;

    private static final double EXPECTED_FPP = 0.05;

    public static BloomFilter createOnHeapBloomFilter(int numExpectedEntries) {
        int byteSize =
                (int)
                        Math.ceil(
                                BloomFilter.optimalNumOfBits(numExpectedEntries, EXPECTED_FPP)
                                        / 8D);
        final BloomFilter filter = new BloomFilter(numExpectedEntries, byteSize);
        filter.setBitsLocation(MemorySegmentFactory.allocateUnpooledSegment(byteSize), 0);
        return filter;
    }

    public static RowData convertBloomFilterToRowData(
            int actualRowCount, @Nullable BloomFilter bloomFilter) {
        return convertBloomFilterToRowData(
                actualRowCount, bloomFilter == null ? null : BloomFilter.toBytes(bloomFilter));
    }

    public static RowData convertBloomFilterToRowData(
            int actualRowCount, @Nullable byte[] serializedBloomFilter) {
        final GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, actualRowCount);
        rowData.setField(1, serializedBloomFilter);
        return rowData;
    }
}
