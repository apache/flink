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

package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.RelativeClock;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link RelativeClock} whose time progress with respect to the wall clock can be paused and
 * un-paused. It can be paused multiple times. If it is paused N times, it has to be un-paused also
 * N times to resume progress.
 */
@Internal
@ThreadSafe
public class PausableRelativeClock implements RelativeClock, TimerGauge.StartStopListener {
    private final Clock baseClock;

    private long accumulativeBlockedNanoTime;
    private long currentBlockedNanoTimeStart;
    /** How many times this clock has been paused. */
    private long pausedCounter;

    public PausableRelativeClock(Clock baseClock) {
        this.baseClock = baseClock;
    }

    @Override
    public long relativeTimeMillis() {
        return relativeTimeNanos() / 1_000_000;
    }

    @Override
    public synchronized long relativeTimeNanos() {
        long now = baseClock.relativeTimeNanos();
        // we offset relativeTimeNanos by the time this clock has been paused.
        return now - getBlockedTime(now);
    }

    /** @return how long this {@link PausableRelativeClock} has been paused so far. */
    private long getBlockedTime(long now) {
        long blockedTime = accumulativeBlockedNanoTime;
        if (pausedCounter != 0) {
            // If we are paused right now, add the time since when we were paused
            blockedTime += now - currentBlockedNanoTimeStart;
        }
        return blockedTime;
    }

    public synchronized void pause() {
        if (pausedCounter == 0) {
            currentBlockedNanoTimeStart = baseClock.relativeTimeNanos();
        }
        pausedCounter++;
    }

    public synchronized void unPause() {
        checkState(pausedCounter >= 1);
        pausedCounter--;
        if (pausedCounter == 0) {
            accumulativeBlockedNanoTime +=
                    baseClock.relativeTimeNanos() - currentBlockedNanoTimeStart;
        }
    }

    @Override
    public void markStart() {
        pause();
    }

    @Override
    public void markEnd() {
        unPause();
    }
}
