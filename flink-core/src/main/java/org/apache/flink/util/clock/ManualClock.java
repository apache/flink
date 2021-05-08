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

package org.apache.flink.util.clock;

import org.apache.flink.annotation.PublicEvolving;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** A {@link Clock} implementation which allows to advance time manually. */
@PublicEvolving
public final class ManualClock extends Clock {

    private final AtomicLong currentTime;

    public ManualClock() {
        this(0);
    }

    public ManualClock(long startTime) {
        this.currentTime = new AtomicLong(startTime);
    }

    @Override
    public long absoluteTimeMillis() {
        return currentTime.get() / 1_000_000L;
    }

    @Override
    public long relativeTimeMillis() {
        return currentTime.get() / 1_000_000L;
    }

    @Override
    public long relativeTimeNanos() {
        return currentTime.get();
    }

    /**
     * Advances the time by the given duration. Time can also move backwards by supplying a negative
     * value. This method performs no overflow check.
     */
    public void advanceTime(long duration, TimeUnit timeUnit) {
        currentTime.addAndGet(timeUnit.toNanos(duration));
    }

    /**
     * Advances the time by the given duration. Time can also move backwards by supplying a negative
     * value. This method performs no overflow check.
     */
    public void advanceTime(Duration duration) {
        currentTime.addAndGet(duration.toNanos());
    }
}
