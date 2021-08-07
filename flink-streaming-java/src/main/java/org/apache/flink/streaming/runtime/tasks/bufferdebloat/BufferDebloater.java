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

package org.apache.flink.streaming.runtime.tasks.bufferdebloat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;

import java.time.Duration;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_TARGET;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Class for automatic calculation of the buffer size based on the current throughput and
 * configuration.
 */
public class BufferDebloater {
    private static final long MILLIS_IN_SECOND = 1000;

    private final Duration targetTotalBufferSize;
    private final IndexedInputGate[] inputGates;
    private final long maxBufferSize;
    private final long minBufferSize;
    private final int bufferDebloatThresholdPercentages;

    private int lastBufferSize;
    private Duration lastEstimatedTimeToConsumeBuffers = Duration.ZERO;

    public BufferDebloater(Configuration taskConfig, IndexedInputGate[] inputGates) {
        this.inputGates = inputGates;
        this.targetTotalBufferSize = taskConfig.get(BUFFER_DEBLOAT_TARGET);
        this.maxBufferSize = taskConfig.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes();
        this.minBufferSize = taskConfig.get(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE).getBytes();

        this.bufferDebloatThresholdPercentages =
                taskConfig.getInteger(BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES);

        this.lastBufferSize = (int) maxBufferSize;

        // Right now the buffer size can not be grater than integer max value according to
        // MemorySegment and buffer implementation.
        checkArgument(maxBufferSize <= Integer.MAX_VALUE);
        checkArgument(maxBufferSize > 0);
        checkArgument(minBufferSize > 0);
        checkArgument(maxBufferSize >= minBufferSize);
        checkArgument(targetTotalBufferSize.toMillis() > 0.0);
    }

    public void recalculateBufferSize(long currentThroughput) {
        long desiredTotalBufferSizeInBytes =
                (currentThroughput * targetTotalBufferSize.toMillis()) / MILLIS_IN_SECOND;

        int totalNumber = 0;
        for (IndexedInputGate inputGate : inputGates) {
            totalNumber += Math.max(1, inputGate.getBuffersInUseCount());
        }
        int newSize =
                (int)
                        Math.max(
                                minBufferSize,
                                Math.min(
                                        desiredTotalBufferSizeInBytes / totalNumber,
                                        maxBufferSize));
        lastEstimatedTimeToConsumeBuffers =
                Duration.ofMillis(
                        newSize * totalNumber * MILLIS_IN_SECOND / Math.max(1, currentThroughput));

        boolean skipUpdate =
                Math.abs(1 - ((double) lastBufferSize) / newSize) * 100
                        < bufferDebloatThresholdPercentages;

        // Skip update if the new value pretty close to the old one.
        if (skipUpdate) {
            return;
        }

        lastBufferSize = newSize;
        for (IndexedInputGate inputGate : inputGates) {
            inputGate.announceBufferSize(newSize);
        }
    }

    public int getLastBufferSize() {
        return lastBufferSize;
    }

    public Duration getLastEstimatedTimeToConsumeBuffers() {
        return lastEstimatedTimeToConsumeBuffers;
    }
}
