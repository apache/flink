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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayDeque;

/**
 * Allows to sample latest watermark values and store those samples in a ring buffer. Returns {@link
 * Watermark.UNINITIALIZED} until fully initialized.
 *
 * <p>Special case for capacity = 0, then sampling is not used, and then always latest value is
 * reported as oldest
 */
@Internal
public class WatermarkSampler {
    private final ArrayDeque<Long> watermarksRingBuffer;
    private final int capacity;
    private long latestWatermark = Watermark.UNINITIALIZED.getTimestamp();

    public WatermarkSampler(int capacity) {
        this.capacity = capacity;
        watermarksRingBuffer = new ArrayDeque<>(capacity);
        for (int i = 0; i < capacity; i++) {
            watermarksRingBuffer.add(Watermark.UNINITIALIZED.getTimestamp());
        }
    }

    public void addLatest(long watermark) {
        latestWatermark = watermark;
    }

    public void sample() {
        watermarksRingBuffer.add(latestWatermark);
        watermarksRingBuffer.remove();
    }

    public long getLatest() {
        return latestWatermark;
    }

    public long getOldestSample() {
        if (capacity == 0) {
            return getLatest();
        }
        return watermarksRingBuffer.getFirst();
    }
}
