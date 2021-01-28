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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Scheduler for recompute and re-triggering tasks to trigger after tasks finished.
 *
 * <p>For each execution, it would maintains the counts of its snapshot reports or decline messages
 * that are still under processing. It would only consider its termination after all these messages
 * are processed. This would simplify the judgement of whether it has reported snapshot before
 * finished.
 *
 * <p>The re-computing could only be trigger when
 *
 * <ol>
 *   <li>Has at least one finished task to consider.
 *   <li>Has passed enough longer interval since last time.
 *   <li>Not in between the triggering process of one checkpoint.
 * </ol>
 */
public class CheckpointBriefRecomputeScheduler {

    private final long minInterval;

    private final ScheduledExecutor timerExecutor;

    private final Supplier<Long> timeQuerier;

    private final Supplier<Boolean> isTriggeringQuerier;

    private final Consumer<List<ExecutionAttemptID>> recomputeFunction;

    private final Map<ExecutionAttemptID, InProgressReportsCount> inProgressReportsCounts =
            new HashMap<>();

    private final Deque<ExecutionAttemptID> finishedTasks = new ArrayDeque<>();

    private long lastRecomputeTime;

    private boolean hasScheduledCheck;

    public CheckpointBriefRecomputeScheduler(
            long minInterval,
            ScheduledExecutor timerExecutor,
            Supplier<Long> timeQuerier,
            Supplier<Boolean> isTriggeringQuerier,
            Consumer<List<ExecutionAttemptID>> recomputeFunction) {
        checkArgument(minInterval >= 0, "min-interval should be non-negative.");
        this.minInterval = minInterval;
        this.timerExecutor = checkNotNull(timerExecutor);
        this.timeQuerier = checkNotNull(timeQuerier);
        this.isTriggeringQuerier = checkNotNull(isTriggeringQuerier);
        this.recomputeFunction = checkNotNull(recomputeFunction);
    }

    void onTaskReport(ExecutionAttemptID executionId, CompletableFuture<?> reportFuture) {
        timerExecutor.execute(
                () -> {
                    InProgressReportsCount count = inProgressReportsCounts.get(executionId);

                    if (count == null) {
                        count = new InProgressReportsCount(1);
                        inProgressReportsCounts.put(executionId, count);
                    } else {
                        count.increase();
                    }

                    reportFuture.thenAcceptAsync(
                            ignored -> {
                                InProgressReportsCount toDecreaseCount =
                                        inProgressReportsCounts.get(executionId);
                                toDecreaseCount.decrease();
                                if (toDecreaseCount.isZero()) {
                                    inProgressReportsCounts.remove(executionId);
                                }
                            },
                            timerExecutor);
                });
    }

    void onTaskFinished(ExecutionAttemptID executionId) {
        timerExecutor.execute(
                () -> {
                    InProgressReportsCount count = inProgressReportsCounts.get(executionId);

                    if (count == null) {
                        finishedTasks.add(executionId);
                        tryScheduleReCompute();
                    } else {
                        count.getZeroFuture()
                                .thenAccept(
                                        (ignored) -> {
                                            finishedTasks.add(executionId);
                                            tryScheduleReCompute();
                                        });
                    }
                });
    }

    void onTriggerFinished() {
        timerExecutor.execute(this::tryScheduleReCompute);
    }

    private void tryScheduleReCompute() {
        if (finishedTasks.size() == 0 || hasScheduledCheck || isTriggeringQuerier.get()) {
            return;
        }

        long now = timeQuerier.get();
        long timePassed = now - lastRecomputeTime;
        if (timePassed < minInterval) {
            timerExecutor.schedule(
                    () -> {
                        hasScheduledCheck = false;
                        tryScheduleReCompute();
                    },
                    minInterval - timePassed,
                    TimeUnit.MILLISECONDS);
            hasScheduledCheck = true;
        } else {
            lastRecomputeTime = now;

            List<ExecutionAttemptID> tasksToRecompute = new ArrayList<>(finishedTasks);
            finishedTasks.clear();

            recomputeFunction.accept(tasksToRecompute);
        }
    }

    private static class InProgressReportsCount {
        private final CompletableFuture<Void> zeroFuture = new CompletableFuture<>();

        private int count;

        public InProgressReportsCount(int count) {
            checkState(count > 0);
            this.count = count;
        }

        public void increase() {
            checkState(count > 0);
            count += 1;
        }

        public void decrease() {
            checkState(count > 0);
            count -= 1;

            if (count == 0) {
                zeroFuture.complete(null);
            }
        }

        public boolean isZero() {
            return count == 0;
        }

        public CompletableFuture<Void> getZeroFuture() {
            return zeroFuture;
        }
    }
}
