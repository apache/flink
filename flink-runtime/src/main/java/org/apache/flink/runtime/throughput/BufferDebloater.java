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

package org.apache.flink.runtime.throughput;

import java.time.Duration;
import java.util.OptionalInt;

/**
 * Class for automatic calculation of the buffer size based on the current throughput and
 * configuration.
 */
public class BufferDebloater {
    private static final long MILLIS_IN_SECOND = 1000;

    private final long targetTotalBufferSize;
    private final int maxBufferSize;
    private final int minBufferSize;
    private final int bufferDebloatThresholdPercentages;
    private final BufferSizeEMA bufferSizeEMA;

    private Duration lastEstimatedTimeToConsumeBuffers = Duration.ZERO;
    private int lastBufferSize;

    public BufferDebloater(
            long targetTotalBufferSize,
            int maxBufferSize,
            int minBufferSize,
            int bufferDebloatThresholdPercentages,
            long numberOfSamples) {
        this.targetTotalBufferSize = targetTotalBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.minBufferSize = minBufferSize;
        this.bufferDebloatThresholdPercentages = bufferDebloatThresholdPercentages;

        this.lastBufferSize = maxBufferSize;
        bufferSizeEMA = new BufferSizeEMA(maxBufferSize, minBufferSize, numberOfSamples);
    }

    public OptionalInt recalculateBufferSize(long currentThroughput, int buffersInUse) {
        int actualBuffersInUse = Math.max(1, buffersInUse);
        long desiredTotalBufferSizeInBytes =
                (currentThroughput * targetTotalBufferSize) / MILLIS_IN_SECOND;

        int newSize =
                bufferSizeEMA.calculateBufferSize(
                        desiredTotalBufferSizeInBytes, actualBuffersInUse);

        lastEstimatedTimeToConsumeBuffers =
                Duration.ofMillis(
                        newSize
                                * actualBuffersInUse
                                * MILLIS_IN_SECOND
                                / Math.max(1, currentThroughput));

        boolean skipUpdate =
                newSize == lastBufferSize
                        || (newSize > minBufferSize && newSize < maxBufferSize)
                                && Math.abs(1 - ((double) lastBufferSize) / newSize) * 100
                                        < bufferDebloatThresholdPercentages;

        // Skip update if the new value pretty close to the old one.
        if (skipUpdate) {
            return OptionalInt.empty();
        }

        lastBufferSize = newSize;
        return OptionalInt.of(newSize);
    }

    public int getLastBufferSize() {
        return lastBufferSize;
    }

    public Duration getLastEstimatedTimeToConsumeBuffers() {
        return lastEstimatedTimeToConsumeBuffers;
    }
}
