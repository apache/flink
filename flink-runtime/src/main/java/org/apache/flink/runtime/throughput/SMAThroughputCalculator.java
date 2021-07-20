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

import static org.apache.flink.runtime.throughput.AverageThroughputCalculator.instantThroughput;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Implementation of 'Simple moving average' algorithm. */
public class SMAThroughputCalculator implements AverageThroughputCalculator {
    /** The sum of the last throughputs.length throughputs. */
    private long totalThroughput;
    /** The buffer of the last calculated throughput. */
    private long[] throughputs;
    /** Current position in the throughputs. */
    private int currentIndex = -1;
    /** The number of last values which should be taken into account during calculation. */
    private int numberOfPreviousValues = 0;

    /**
     * @param numberOfPreviousValues How many previous values should be taken into account during
     *     the calculation.
     */
    public SMAThroughputCalculator(int numberOfPreviousValues) {
        checkState(numberOfPreviousValues > 0, "Number of previous values should be positive.");
        throughputs = new long[numberOfPreviousValues];
    }

    @Override
    public long calculateThroughput(long dataSize, long time) {
        checkArgument(dataSize >= 0, "Size of data should be non negative");
        checkArgument(time >= 0, "Time should be non negative");

        if (time == 0) {
            return numberOfPreviousValues == 0 ? 0 : totalThroughput / numberOfPreviousValues;
        }

        long newThroughput = instantThroughput(dataSize, time);

        currentIndex = (currentIndex + 1) % throughputs.length;
        totalThroughput -= throughputs[currentIndex];
        totalThroughput += (throughputs[currentIndex] = newThroughput);

        if (numberOfPreviousValues < throughputs.length) {
            numberOfPreviousValues++;
        }

        return totalThroughput / numberOfPreviousValues;
    }

    boolean isWarmedUp() {
        return numberOfPreviousValues == throughputs.length;
    }
}
