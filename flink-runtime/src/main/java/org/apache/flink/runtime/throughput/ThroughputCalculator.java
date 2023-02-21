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

import static org.apache.flink.util.Preconditions.checkArgument;

/** Class for measuring the throughput based on incoming data size and measurement period. */
public class ThroughputCalculator {
    private static final long NOT_TRACKED = -1;
    private final Clock clock;
    private static final long MILLIS_IN_SECOND = 1000;
    private long currentThroughput;

    private long currentAccumulatedDataSize;
    private long currentMeasurementTime;
    private long measurementStartTime = NOT_TRACKED;

    public ThroughputCalculator(Clock clock) {
        this.clock = clock;
    }

    public void incomingDataSize(long receivedDataSize) {
        // Force resuming measurement.
        if (measurementStartTime == NOT_TRACKED) {
            measurementStartTime = clock.absoluteTimeMillis();
        }
        currentAccumulatedDataSize += receivedDataSize;
    }

    /** Mark when the time should not be taken into account. */
    public void pauseMeasurement() {
        if (measurementStartTime != NOT_TRACKED) {
            currentMeasurementTime += clock.absoluteTimeMillis() - measurementStartTime;
        }
        measurementStartTime = NOT_TRACKED;
    }

    /** Mark when the time should be included to the throughput calculation. */
    public void resumeMeasurement() {
        if (measurementStartTime == NOT_TRACKED) {
            measurementStartTime = clock.absoluteTimeMillis();
        }
    }

    /** @return Calculated throughput based on the collected data for the last period. */
    public long calculateThroughput() {
        if (measurementStartTime != NOT_TRACKED) {
            long absoluteTimeMillis = clock.absoluteTimeMillis();
            currentMeasurementTime += absoluteTimeMillis - measurementStartTime;
            measurementStartTime = absoluteTimeMillis;
        }

        long throughput = calculateThroughput(currentAccumulatedDataSize, currentMeasurementTime);

        currentAccumulatedDataSize = currentMeasurementTime = 0;

        return throughput;
    }

    public long calculateThroughput(long dataSize, long time) {
        checkArgument(dataSize >= 0, "Size of data should be non negative");
        checkArgument(time >= 0, "Time should be non negative");

        if (time == 0) {
            return currentThroughput;
        }

        return currentThroughput = instantThroughput(dataSize, time);
    }

    static long instantThroughput(long dataSize, long time) {
        return (long) ((double) dataSize / time * MILLIS_IN_SECOND);
    }
}
