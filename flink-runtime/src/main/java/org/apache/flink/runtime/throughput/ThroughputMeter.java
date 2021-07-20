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

import org.apache.flink.util.clock.Clock;

/** Class for measuring the throughput based on incoming data size and busy + backpressure time. */
public class ThroughputMeter {
    private static final long NOT_TRACKED = -1;
    private final Clock clock;
    private final AverageThroughputCalculator averageThroughputCalculator;

    /** Accumulated data size for current period. */
    private long currentSize;

    /** Total busy time for current period(the total time minus idle time). */
    private long currentTime;

    /** The start of busy time. */
    private long startTime;

    /** The last throughput . */
    private long effectiveThroughput;

    public ThroughputMeter(Clock clock, AverageThroughputCalculator averageThroughputCalculator) {
        this.clock = clock;
        this.averageThroughputCalculator = averageThroughputCalculator;
    }

    /** @param size The size of received data. */
    public void incomingDataSize(long size) {
        currentSize += size;
    }

    /** Mark when idle time is started. */
    public void idleStart() {
        if (startTime != NOT_TRACKED) {
            currentTime += clock.relativeTimeMillis() - startTime;
        }
        startTime = NOT_TRACKED;
    }

    /** Mark when idle time is ended. */
    public void idleEnd() {
        startTime = clock.relativeTimeMillis();
    }

    /**
     * Calculate throughput based on the collected data for the last period.
     *
     * @return {@code -1} if it is impossible to calculate throughput and actual value otherwise.
     */
    public long calculateThroughput() {
        if (startTime != NOT_TRACKED) {
            currentTime += clock.relativeTimeMillis() - startTime;
        }

        effectiveThroughput =
                averageThroughputCalculator.calculateThroughput(currentSize, currentTime);

        startTime = clock.relativeTimeMillis();
        currentSize = currentTime = 0;

        return effectiveThroughput;
    }
}
