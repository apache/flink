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
public class ThroughputEMA {
    private static final long MILLIS_IN_SECOND = 1000;
    private long currentThroughput;

    /** EMA algorithm specific constant which responsible for speed of reaction. */
    private final double alpha;

    public ThroughputEMA(long numberOfSamples) {
        alpha = 2.0 / (numberOfSamples + 1);
    }

    /**
     * Calculating the throughput over the size of data which were received in given amount of time.
     *
     * @param dataSize Size of the data which were received in given time.
     * @param time Time during which the data was received.
     * @return Throughput calculated according to implemented algorithm.
     */
    public long calculateThroughput(long dataSize, long time) {
        checkArgument(dataSize >= 0, "Size of data should be non negative");
        checkArgument(time >= 0, "Time should be non negative");

        if (time == 0) {
            return currentThroughput;
        }

        if (currentThroughput != 0) {
            currentThroughput += alpha * (instantThroughput(dataSize, time) - currentThroughput);
        } else {
            // Warm-up case.
            currentThroughput = instantThroughput(dataSize, time);
        }

        return currentThroughput;
    }

    static long instantThroughput(long dataSize, long time) {
        return (long) ((double) dataSize / time * MILLIS_IN_SECOND);
    }
}
