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

package org.apache.flink.runtime.operators.sort;

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 */
public final class HeapSort implements IndexedSorter {
    public HeapSort() {}

    /**
     * @param s the record array to adjust the heap.
     * @param b offset of the index. If the origin index in the array is 0 and it's logical location
     *     is 1, it should set -1 to adjust the logical index to physical index.
     * @param i logical index in the heap
     * @param boundary boundary of the heap in logical
     */
    public void downHeap(final IndexedSortable s, final int b, int i, final int boundary) {
        for (int idx = i << 1; idx < boundary; idx = i << 1) {
            if (idx + 1 < boundary && s.compare(b + idx, b + idx + 1) < 0) {
                if (s.compare(b + i, b + idx + 1) < 0) {
                    s.swap(b + i, b + idx + 1);
                } else {
                    return;
                }
                i = idx + 1;
            } else if (s.compare(b + i, b + idx) < 0) {
                s.swap(b + i, b + idx);
                i = idx;
            } else {
                return;
            }
        }
    }

    /**
     * Make the buffer as a heap.
     *
     * @param p the index of the first element to sort in physical.
     * @param r the last element index to sort (not included) in physical.
     */
    public void heapify(final IndexedSortable s, final int p, final int r) {
        final int recordNum = r - p;
        // build heap w/ reverse comparator, then write in-place from end
        final int t = Integer.highestOneBit(recordNum);
        for (int i = t; i > 1; i >>>= 1) {
            for (int j = i >>> 1; j < i; ++j) {
                downHeap(s, p - 1, j, recordNum + 1);
            }
        }
    }

    /**
     * Bottom-up heap sort. After building the heap, sort the data in memory.
     *
     * @param p the index of the first element to sort.
     * @param r the last element index to sort (not included)
     */
    public void sort(final IndexedSortable s, final int p, final int r) {
        heapify(s, p, r);
        for (int i = r - 1; i > p; --i) {
            s.swap(p, i);
            downHeap(s, p - 1, 1, i - p + 1);
        }
    }

    @Override
    public void sort(IndexedSortable s) {
        sort(s, 0, s.size());
    }
}
