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

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.OptionalInt;

/**
 * Class for automatic calculation of the buffer size based on the current throughput and
 * configuration.
 */
public class BufferDebloater {
    private static final Logger LOG = LoggerFactory.getLogger(BufferDebloater.class);
    private static final long MILLIS_IN_SECOND = 1000;

    private final String owningTaskName;
    private final int gateIndex;
    private final long targetTotalTime;
    private final int maxBufferSize;
    private final int minBufferSize;
    private final double bufferDebloatThresholdFactor;
    private final BufferSizeEMA bufferSizeEMA;

    private Duration lastEstimatedTimeToConsumeBuffers = Duration.ZERO;
    private int lastBufferSize;

    public BufferDebloater(
            String owningTaskName,
            int gateIndex,
            long targetTotalTime,
            int maxBufferSize,
            int minBufferSize,
            int bufferDebloatThresholdPercentages,
            long numberOfSamples) {
        this(
                owningTaskName,
                gateIndex,
                targetTotalTime,
                maxBufferSize,
                maxBufferSize,
                minBufferSize,
                bufferDebloatThresholdPercentages,
                numberOfSamples);
    }

    public BufferDebloater(
            String owningTaskName,
            int gateIndex,
            long targetTotalTime,
            int startingBufferSize,
            int maxBufferSize,
            int minBufferSize,
            int bufferDebloatThresholdPercentages,
            long numberOfSamples) {
        this.owningTaskName = owningTaskName;
        this.gateIndex = gateIndex;
        this.targetTotalTime = targetTotalTime;
        this.maxBufferSize = maxBufferSize;
        this.minBufferSize = minBufferSize;
        this.bufferDebloatThresholdFactor = bufferDebloatThresholdPercentages / 100.0;

        this.lastBufferSize = startingBufferSize;
        bufferSizeEMA =
                new BufferSizeEMA(
                        startingBufferSize, maxBufferSize, minBufferSize, numberOfSamples);

        LOG.debug(
                "{}: Buffer debloater init settings: gateIndex={}, targetTotalTime={}, maxBufferSize={}, minBufferSize={}, bufferDebloatThresholdPercentages={}, numberOfSamples={}",
                owningTaskName,
                gateIndex,
                targetTotalTime,
                maxBufferSize,
                minBufferSize,
                bufferDebloatThresholdPercentages,
                numberOfSamples);
    }

    public OptionalInt recalculateBufferSize(long currentThroughput, int buffersInUse) {
        int actualBuffersInUse = Math.max(1, buffersInUse);
        long desiredTotalBufferSizeInBytes =
                (currentThroughput * targetTotalTime) / MILLIS_IN_SECOND;

        int newSize =
                bufferSizeEMA.calculateBufferSize(
                        desiredTotalBufferSizeInBytes, actualBuffersInUse);

        lastEstimatedTimeToConsumeBuffers =
                Duration.ofMillis(
                        newSize
                                * actualBuffersInUse
                                * MILLIS_IN_SECOND
                                / Math.max(1, currentThroughput));

        boolean skipUpdate = skipUpdate(newSize);

        LOG.debug(
                "{}: Buffer size recalculation: gateIndex={}, currentSize={}, newSize={}, instantThroughput={}, desiredBufferSize={}, buffersInUse={}, estimatedTimeToConsumeBuffers={}, announceNewSize={}",
                owningTaskName,
                gateIndex,
                lastBufferSize,
                newSize,
                currentThroughput,
                desiredTotalBufferSizeInBytes,
                buffersInUse,
                lastEstimatedTimeToConsumeBuffers,
                !skipUpdate);

        // Skip update if the new value pretty close to the old one.
        if (skipUpdate) {
            return OptionalInt.empty();
        }

        lastBufferSize = newSize;
        return OptionalInt.of(newSize);
    }

    @VisibleForTesting
    boolean skipUpdate(int newSize) {
        if (newSize == lastBufferSize) {
            return true;
        }

        // According to logic of this class newSize can not be less than min or greater than max
        // buffer size but if considering this method independently the behaviour for the small or
        // big value should be the same as for min and max buffer size correspondingly.
        if (newSize <= minBufferSize || newSize >= maxBufferSize) {
            return false;
        }

        int delta = (int) (lastBufferSize * bufferDebloatThresholdFactor);
        return Math.abs(newSize - lastBufferSize) < delta;
    }

    public int getLastBufferSize() {
        return lastBufferSize;
    }

    public Duration getLastEstimatedTimeToConsumeBuffers() {
        return lastEstimatedTimeToConsumeBuffers;
    }
}
