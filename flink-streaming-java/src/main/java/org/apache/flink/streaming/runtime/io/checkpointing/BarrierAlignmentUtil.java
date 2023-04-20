/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.clock.Clock;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;

/** Utility for barrier alignment. */
@Internal
public class BarrierAlignmentUtil {

    public static long getTimerDelay(Clock clock, CheckpointBarrier announcedBarrier) {
        long alignedCheckpointTimeout =
                announcedBarrier.getCheckpointOptions().getAlignedCheckpointTimeout();
        long timePassedSinceCheckpointStart =
                clock.absoluteTimeMillis() - announcedBarrier.getTimestamp();

        return Math.max(alignedCheckpointTimeout - timePassedSinceCheckpointStart, 0);
    }

    public static DelayableTimer createRegisterTimerCallback(
            MailboxExecutor mailboxExecutor, TimerService timerService) {
        return (callable, delay) -> {
            ScheduledFuture<?> scheduledFuture =
                    timerService.registerTimer(
                            timerService.getCurrentProcessingTime() + delay.toMillis(),
                            timestamp ->
                                    mailboxExecutor.execute(
                                            () -> callable.call(),
                                            "Execute checkpoint barrier handler delayed action"));
            return () -> scheduledFuture.cancel(false);
        };
    }

    /** It can register a task to be executed some time later. */
    public interface DelayableTimer {

        /**
         * Register a task to be executed some time later.
         *
         * @param callable the task to submit
         * @param delay how long after the delay to execute the task
         * @return the Cancellable, it can cancel the task.
         */
        Cancellable registerTask(Callable<?> callable, Duration delay);
    }

    /** A handle to a delayed action which can be cancelled. */
    public interface Cancellable {
        void cancel();
    }
}
