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
    private final Duration targetTotalBufferSize;
    private final int maxBufferSize;
    private final int minBufferSize;
    private final int bufferDebloatThresholdPercentages;
    private final int numberOfSamples;
    private final boolean enabled;

    private BufferDebloatConfiguration(
            boolean enabled,
            Duration targetTotalBufferSize,
            int maxBufferSize,
            int minBufferSize,
            int bufferDebloatThresholdPercentages,
            int numberOfSamples) {
        this.targetTotalBufferSize = checkNotNull(targetTotalBufferSize);
        this.maxBufferSize = maxBufferSize;
        this.minBufferSize = minBufferSize;
        this.bufferDebloatThresholdPercentages = bufferDebloatThresholdPercentages;
        this.numberOfSamples = numberOfSamples;
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Duration getTargetTotalBufferSize() {
        return targetTotalBufferSize;
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
        Duration targetTotalBufferSize = config.get(BUFFER_DEBLOAT_TARGET);
        int maxBufferSize =
                Math.toIntExact(config.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes());
        int minBufferSize =
                Math.toIntExact(config.get(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE).getBytes());

        int bufferDebloatThresholdPercentages = config.get(BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES);
        final int numberOfSamples = config.get(BUFFER_DEBLOAT_SAMPLES);

        // Right now the buffer size can not be grater than integer max value according to
        // MemorySegment and buffer implementation.
        checkArgument(maxBufferSize > 0);
        checkArgument(minBufferSize > 0);
        checkArgument(numberOfSamples > 0);
        checkArgument(maxBufferSize >= minBufferSize);
        checkArgument(targetTotalBufferSize.toMillis() > 0.0);
        return new BufferDebloatConfiguration(
                config.get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED),
                targetTotalBufferSize,
                maxBufferSize,
                minBufferSize,
                bufferDebloatThresholdPercentages,
                numberOfSamples);
    }
}
