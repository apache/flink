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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a {@link TimerService} and {@link ProcessingTimeService} used <b>strictly for testing</b>
 * the processing time functionality.
 */
public class TestProcessingTimeService implements TimerService {

    private volatile boolean isTerminated;
    private volatile boolean isQuiesced;

    // sorts the timers by timestamp so that they are processed in the correct order.
    private final PriorityQueue<Tuple2<Long, CallbackTask>> priorityQueue;

    private final ManualMSClock clock = new ManualMSClock(Long.MIN_VALUE);

    public TestProcessingTimeService() {
        this.priorityQueue =
                new PriorityQueue<>(
                        16,
                        new Comparator<Tuple2<Long, CallbackTask>>() {
                            @Override
                            public int compare(
                                    Tuple2<Long, CallbackTask> o1, Tuple2<Long, CallbackTask> o2) {
                                return Long.compare(o1.f0, o2.f0);
                            }
                        });
    }

    public void advance(long delta) throws Exception {
        clock.advanceTime(Duration.ofMillis(delta));
        maybeFireTimers();
    }

    public void setCurrentTime(long timestamp) throws Exception {
        clock.setCurrentTime(timestamp, TimeUnit.MILLISECONDS);
        maybeFireTimers();
    }

    private void maybeFireTimers() throws Exception {
        if (!isQuiesced) {
            while (!priorityQueue.isEmpty()
                    && getCurrentProcessingTime() >= priorityQueue.peek().f0) {
                Tuple2<Long, CallbackTask> entry = priorityQueue.poll();

                CallbackTask callbackTask = entry.f1;

                if (!callbackTask.isDone()) {
                    callbackTask.onProcessingTime(entry.f0);

                    if (callbackTask instanceof PeriodicCallbackTask) {
                        priorityQueue.offer(
                                Tuple2.of(
                                        ((PeriodicCallbackTask) callbackTask)
                                                .nextTimestamp(entry.f0),
                                        callbackTask));
                    }
                }
            }
        }
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
        if (isTerminated) {
            throw new IllegalStateException("terminated");
        }
        if (isQuiesced) {
            return new CallbackTask(null);
        }

        CallbackTask callbackTask = new CallbackTask(target);

        priorityQueue.offer(Tuple2.of(timestamp, callbackTask));

        return callbackTask;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            ProcessingTimeCallback callback, long initialDelay, long period) {
        if (isTerminated) {
            throw new IllegalStateException("terminated");
        }
        if (isQuiesced) {
            return new CallbackTask(null);
        }

        PeriodicCallbackTask periodicCallbackTask = new PeriodicCallbackTask(callback, period);

        priorityQueue.offer(
                Tuple2.<Long, CallbackTask>of(
                        getCurrentProcessingTime() + initialDelay, periodicCallbackTask));

        return periodicCallbackTask;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            ProcessingTimeCallback callback, long initialDelay, long period) {
        // for all testing purposed, there is no difference between the fixed rate and fixed delay
        return scheduleAtFixedRate(callback, initialDelay, period);
    }

    @Override
    public boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public CompletableFuture<Void> quiesce() {
        if (!isTerminated) {
            isQuiesced = true;
            priorityQueue.clear();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void shutdownService() {
        this.isTerminated = true;
    }

    @Override
    public boolean shutdownServiceUninterruptible(long timeoutMs) {
        shutdownService();
        return true;
    }

    public int getNumActiveTimers() {
        int count = 0;

        for (Tuple2<Long, CallbackTask> entry : priorityQueue) {
            if (!entry.f1.isDone()) {
                count++;
            }
        }

        return count;
    }

    public Set<Long> getActiveTimerTimestamps() {
        Set<Long> actualTimestamps = new HashSet<>();

        for (Tuple2<Long, CallbackTask> entry : priorityQueue) {
            if (!entry.f1.isDone()) {
                actualTimestamps.add(entry.f0);
            }
        }

        return actualTimestamps;
    }

    // ------------------------------------------------------------------------

    private static class CallbackTask implements ScheduledFuture<Object> {

        protected final ProcessingTimeCallback processingTimeCallback;

        private AtomicReference<CallbackTaskState> state =
                new AtomicReference<>(CallbackTaskState.CREATED);

        private CallbackTask(ProcessingTimeCallback processingTimeCallback) {
            this.processingTimeCallback = processingTimeCallback;
        }

        public void onProcessingTime(long timestamp) throws Exception {
            processingTimeCallback.onProcessingTime(timestamp);

            state.compareAndSet(CallbackTaskState.CREATED, CallbackTaskState.DONE);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return state.compareAndSet(CallbackTaskState.CREATED, CallbackTaskState.CANCELLED);
        }

        @Override
        public boolean isCancelled() {
            return state.get() == CallbackTaskState.CANCELLED;
        }

        @Override
        public boolean isDone() {
            return state.get() != CallbackTaskState.CREATED;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        enum CallbackTaskState {
            CREATED,
            CANCELLED,
            DONE
        }
    }

    private static class PeriodicCallbackTask extends CallbackTask {

        private final long period;

        private PeriodicCallbackTask(ProcessingTimeCallback processingTimeCallback, long period) {
            super(processingTimeCallback);
            Preconditions.checkArgument(period > 0L, "The period must be greater than 0.");

            this.period = period;
        }

        @Override
        public void onProcessingTime(long timestamp) throws Exception {
            processingTimeCallback.onProcessingTime(timestamp);
        }

        public long nextTimestamp(long currentTimestamp) {
            return currentTimestamp + period;
        }
    }

    /**
     * Similar to {@link org.apache.flink.util.clock.ManualClock}, but with ms precision and thus
     * greater range. This is needed to support registering and firing timers with {@link
     * Long#MAX_VALUE}.
     */
    private static class ManualMSClock extends Clock {
        private final AtomicLong currentTime;

        public ManualMSClock(long startTime) {
            this.currentTime = new AtomicLong(startTime);
        }

        @Override
        public long absoluteTimeMillis() {
            return currentTime.get();
        }

        @Override
        public long relativeTimeMillis() {
            return currentTime.get();
        }

        @Override
        public long relativeTimeNanos() {
            return currentTime.get() * 1_000_000;
        }

        /**
         * Advances the time by the given duration. Time can also move backwards by supplying a
         * negative value. This method performs no overflow check.
         */
        public void advanceTime(Duration duration) {
            currentTime.addAndGet(duration.toMillis());
        }

        /** Sets the time to the given value. */
        public void setCurrentTime(long time, TimeUnit timeUnit) {
            currentTime.set(timeUnit.toMillis(time));
        }
    }
}
