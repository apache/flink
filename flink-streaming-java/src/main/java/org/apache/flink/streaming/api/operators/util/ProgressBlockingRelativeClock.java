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
 * A {@link RelativeClock} that can be marked to start and stop blocking progress of the relative
 * time with respect to the wall clock.
 */
@Internal
@ThreadSafe
public class ProgressBlockingRelativeClock implements RelativeClock, TimerGauge.StartStopListener {
    private final Clock baseClock;

    private long accumulativeBlockedNanoTime;
    private long currentBlockedNanoTimeStart;
    private long blockedCounter;

    public ProgressBlockingRelativeClock(Clock baseClock) {
        this.baseClock = baseClock;
    }

    @Override
    public long relativeTimeMillis() {
        return relativeTimeNanos() / 1_000_000;
    }

    @Override
    public long relativeTimeNanos() {
        return baseClock.relativeTimeNanos() - getBlockedTime();
    }

    public synchronized void markBlocked() {
        if (blockedCounter == 0) {
            currentBlockedNanoTimeStart = baseClock.relativeTimeNanos();
        }
        blockedCounter++;
    }

    public synchronized void markUnblocked() {
        checkState(blockedCounter >= 1);
        blockedCounter--;
        if (blockedCounter == 0) {
            accumulativeBlockedNanoTime +=
                    baseClock.relativeTimeNanos() - currentBlockedNanoTimeStart;
        }
    }

    private synchronized long getBlockedTime() {
        return accumulativeBlockedNanoTime + getCurrentBlockedNanoTime();
    }

    private long getCurrentBlockedNanoTime() {
        return blockedCounter == 0
                ? 0
                : baseClock.relativeTimeNanos() - currentBlockedNanoTimeStart;
    }

    @Override
    public void markStart() {
        markBlocked();
    }

    @Override
    public void markEnd() {
        markUnblocked();
    }
}
