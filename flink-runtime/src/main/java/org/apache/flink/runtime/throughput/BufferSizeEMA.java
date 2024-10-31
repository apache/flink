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

import static org.apache.flink.util.Preconditions.checkArgument;

/** Implementation of 'Exponential moving average' algorithm. */
public class BufferSizeEMA {
    private final int maxBufferSize;
    private final int minBufferSize;
    /** EMA algorithm specific constant which responsible for speed of reaction. */
    private final double alpha;

    private double lastBufferSize;

    public BufferSizeEMA(int maxBufferSize, int minBufferSize, long numberOfSamples) {
        this(maxBufferSize, maxBufferSize, minBufferSize, numberOfSamples);
    }

    public BufferSizeEMA(
            int startingBufferSize, int maxBufferSize, int minBufferSize, long numberOfSamples) {
        this.maxBufferSize = maxBufferSize;
        this.minBufferSize = minBufferSize;
        alpha = 2.0 / (numberOfSamples + 1);
        this.lastBufferSize = startingBufferSize;
    }

    /**
     * Calculating the buffer size over total possible buffers size and number of buffers in use.
     *
     * @param totalBufferSizeInBytes Total buffers size.
     * @param totalBuffers Total number of buffers in use.
     * @return Throughput calculated according to implemented algorithm.
     */
    public int calculateBufferSize(long totalBufferSizeInBytes, int totalBuffers) {
        checkArgument(totalBufferSizeInBytes >= 0, "Size of buffer should be non negative");
        checkArgument(totalBuffers > 0, "Number of buffers should be positive");

        // Since the result value is always limited by max buffer size while the instant value is
        // potentially unlimited. It can lead to an instant change from min to max value in case
        // when the instant value is significantly larger than the possible max value.
        // The solution is to limit the instant buffer size by twice of current buffer size in order
        // to have the same growth and shrink speeds. for example if the instant value is equal to 0
        // and the current value is 16000 we can decrease it at maximum by 1600(suppose alfa=0.1) .
        // The idea is to allow increase and decrease size by the same number. So if the instant
        // value would be large(for example 100000) it will be possible to increase the current
        // value by 1600(the same as decreasing) because the limit will be 2 * currentValue = 32000.
        // Example of change speed:
        // growing = 32768, 29647, 26823, 24268, 21956, 19864
        // shrinking = 19864, 21755, 23826, 26095, 28580, 31301, 32768
        double desirableBufferSize =
                Math.min(((double) totalBufferSizeInBytes) / totalBuffers, 2L * lastBufferSize);

        lastBufferSize += alpha * (desirableBufferSize - lastBufferSize);
        lastBufferSize = Math.max(minBufferSize, Math.min(lastBufferSize, maxBufferSize));
        return (int) Math.round(lastBufferSize);
    }
}
