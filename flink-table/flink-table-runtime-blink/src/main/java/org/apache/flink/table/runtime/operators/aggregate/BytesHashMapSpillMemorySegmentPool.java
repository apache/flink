/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.runtime.util.collections.binary.BytesHashMap;

import java.util.List;

/**
 * MemorySegmentPool for {@link BytesHashMap} to fall back to sort agg. {@link #nextSegment} not
 * remove segment from segments, just get from segments.
 */
public class BytesHashMapSpillMemorySegmentPool implements MemorySegmentPool {

    private final List<MemorySegment> segments;
    private final int pageSize;
    private int allocated;

    public BytesHashMapSpillMemorySegmentPool(List<MemorySegment> memorySegments) {
        this.segments = memorySegments;
        this.pageSize = memorySegments.get(0).size();
        this.allocated = 0;
    }

    @Override
    public MemorySegment nextSegment() {
        allocated++;
        if (allocated <= segments.size()) {
            return segments.get(allocated - 1);
        } else {
            return MemorySegmentFactory.wrap(new byte[pageSize()]);
        }
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        throw new UnsupportedOperationException("not support!");
    }

    @Override
    public int freePages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }
}
