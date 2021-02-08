/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.View;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

/**
 * {@link TimerGauge} measures how much time is spent in a given state, with entry into that state
 * being signaled by {@link #markStart()}. Measuring is stopped by {@link #markEnd()}. This class in
 * particularly takes care of the case, when {@link #update()} is called when some measurement
 * started but has not yet finished. For example even if next {@link #markEnd()} call is expected to
 * happen in a couple of hours, the returned value will account for this ongoing measurement.
 */
public class TimerGauge implements Gauge<Long>, View {
    private final Clock clock;

    private long previousCount;
    private long currentCount;
    private long currentMeasurementStart;

    public TimerGauge() {
        this(SystemClock.getInstance());
    }

    public TimerGauge(Clock clock) {
        this.clock = clock;
    }

    public synchronized void markStart() {
        if (currentMeasurementStart == 0) {
            currentMeasurementStart = clock.absoluteTimeMillis();
        }
    }

    public synchronized void markEnd() {
        if (currentMeasurementStart != 0) {
            currentCount += clock.absoluteTimeMillis() - currentMeasurementStart;
            currentMeasurementStart = 0;
        }
    }

    @Override
    public synchronized void update() {
        if (currentMeasurementStart != 0) {
            long now = clock.absoluteTimeMillis();
            currentCount += now - currentMeasurementStart;
            currentMeasurementStart = now;
        }
        previousCount = Math.max(Math.min(currentCount / UPDATE_INTERVAL_SECONDS, 1000), 0);
        currentCount = 0;
    }

    @Override
    public synchronized Long getValue() {
        return previousCount;
    }

    @VisibleForTesting
    public synchronized long getCount() {
        return currentCount;
    }

    @VisibleForTesting
    public synchronized boolean isMeasuring() {
        return currentMeasurementStart != 0;
    }
}
