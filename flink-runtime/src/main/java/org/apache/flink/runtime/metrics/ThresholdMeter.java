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

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A timestamp queue based threshold meter.
 *
 * <p>Note: This class is thread safe, at the price of synchronization overhead. Do not use this in
 * performance sensitive scenarios, e.g., per-record updated metrics.
 */
public class ThresholdMeter implements Meter {
    private static final double MILLISECONDS_PER_SECOND = 1000.0;
    private final Clock clock;
    private final double maxEventsPerInterval;
    private final Duration interval;

    @GuardedBy("this")
    private final Queue<Long> eventTimestamps;

    @GuardedBy("this")
    private long eventCount = 0;

    public ThresholdMeter(double maxEventsPerInterval, Duration interval) {
        this(maxEventsPerInterval, interval, SystemClock.getInstance());
    }

    @VisibleForTesting
    public ThresholdMeter(double maxEventsPerInterval, Duration interval, Clock clock) {
        this.clock = clock;
        this.maxEventsPerInterval = maxEventsPerInterval;
        this.interval = interval;
        this.eventTimestamps = new ArrayDeque<>();
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("The threshold interval should be larger than 0.");
        }
    }

    @Override
    public synchronized void markEvent() {
        eventTimestamps.add(clock.absoluteTimeMillis());
        eventCount++;
    }

    @Override
    public synchronized void markEvent(long n) {
        long timestamp = clock.absoluteTimeMillis();
        for (int i = 0; i < n; i++) {
            eventTimestamps.add(timestamp);
        }
        eventCount = eventCount + n;
    }

    @Override
    public double getRate() {
        return getEventCountsRecentInterval() / (interval.toMillis() / MILLISECONDS_PER_SECOND);
    }

    @Override
    public synchronized long getCount() {
        return eventCount;
    }

    public void checkAgainstThreshold() throws ThresholdExceedException {
        int recentEvents = getEventCountsRecentInterval();
        if (recentEvents >= maxEventsPerInterval) {
            throw new ThresholdExceedException(
                    String.format(
                            "%d events detected in the recent interval, reaching the threshold %f.",
                            recentEvents, maxEventsPerInterval));
        }
    }

    private synchronized int getEventCountsRecentInterval() {
        Long currentTimeStamp = clock.absoluteTimeMillis();
        while (!eventTimestamps.isEmpty()
                && currentTimeStamp - eventTimestamps.peek() > interval.toMillis()) {
            eventTimestamps.remove();
        }

        return eventTimestamps.size();
    }

    /** Exception thrown when a threshold exceeds. */
    public static class ThresholdExceedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ThresholdExceedException(String message) {
            super(message);
        }
    }
}
