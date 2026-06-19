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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.TaskManagerOptions;

import java.time.Duration;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_SAMPLES;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_TARGET;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Configuration for {@link BufferDebloater}. */
public final class BufferDebloatConfiguration {
    private final Duration targetTotalTime;
    private final int startingBufferSize;
    private final int maxBufferSize;
    private final int minBufferSize;
    private final int bufferDebloatThresholdPercentages;
    private final int numberOfSamples;
    private final boolean enabled;

    private BufferDebloatConfiguration(
            boolean enabled,
            Duration targetTotalTime,
            int startingBufferSize,
            int maxBufferSize,
            int minBufferSize,
            int bufferDebloatThresholdPercentages,
            int numberOfSamples) {
        // Right now the buffer size can not be grater than integer max value according to
        // MemorySegment and buffer implementation.
        checkArgument(maxBufferSize > 0);
        checkArgument(minBufferSize > 0);
        checkArgument(numberOfSamples > 0);
        checkArgument(maxBufferSize >= minBufferSize);
        checkArgument(targetTotalTime.toMillis() > 0.0);
        checkArgument(maxBufferSize >= startingBufferSize);
        checkArgument(minBufferSize <= startingBufferSize);

        this.targetTotalTime = checkNotNull(targetTotalTime);
        this.startingBufferSize = startingBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.minBufferSize = minBufferSize;
        this.bufferDebloatThresholdPercentages = bufferDebloatThresholdPercentages;
        this.numberOfSamples = numberOfSamples;
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Duration getTargetTotalTime() {
        return targetTotalTime;
    }

    public int getStartingBufferSize() {
        return startingBufferSize;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getMinBufferSize() {
        return minBufferSize;
    }

    public int getBufferDebloatThresholdPercentages() {
        return bufferDebloatThresholdPercentages;
    }

    public int getNumberOfSamples() {
        return numberOfSamples;
    }

    public static BufferDebloatConfiguration fromConfiguration(ReadableConfig config) {
        Duration targetTotalTime = config.get(BUFFER_DEBLOAT_TARGET);
        int maxBufferSize =
                Math.toIntExact(config.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes());
        int minBufferSize =
                Math.toIntExact(config.get(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE).getBytes());
        int startingBufferSize =
                Math.toIntExact(
                        config.get(TaskManagerOptions.STARTING_MEMORY_SEGMENT_SIZE).getBytes());

        int bufferDebloatThresholdPercentages = config.get(BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES);
        final int numberOfSamples = config.get(BUFFER_DEBLOAT_SAMPLES);

        return new BufferDebloatConfiguration(
                config.get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED),
                targetTotalTime,
                startingBufferSize,
                maxBufferSize,
                minBufferSize,
                bufferDebloatThresholdPercentages,
                numberOfSamples);
    }
}
