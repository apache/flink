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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;
import java.util.Objects;

/**
 * A {@link AsyncTrigger} that can turn any {@link AsyncTrigger} into a timeout {@code
 * AsyncTrigger}.
 *
 * <p>On the first arriving element a configurable processing-time timeout will be set. Using {@link
 * #of(AsyncTrigger, Duration, boolean, boolean)}, you can also re-new the timer for each arriving
 * element by specifying {@code resetTimerOnNewRecord} and you can specify whether {@link
 * AsyncTrigger#clear(Window, AsyncTrigger.TriggerContext)} should be called on timout via {@code
 * shouldClearOnTimeout}.
 *
 * @param <T> The type of elements on which this trigger can operate.
 * @param <W> The type of {@link Window} on which this trigger can operate.
 */
@Experimental
public class AsyncProcessingTimeoutTrigger<T, W extends Window> extends AsyncTrigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final AsyncTrigger<T, W> nestedTrigger;
    private final long interval;
    private final boolean resetTimerOnNewRecord;
    private final boolean shouldClearOnTimeout;

    private final ValueStateDescriptor<Long> timeoutStateDesc;

    public AsyncProcessingTimeoutTrigger(
            AsyncTrigger<T, W> nestedTrigger,
            long interval,
            boolean resetTimerOnNewRecord,
            boolean shouldClearOnTimeout) {
        this.nestedTrigger = nestedTrigger;
        this.interval = interval;
        this.resetTimerOnNewRecord = resetTimerOnNewRecord;
        this.shouldClearOnTimeout = shouldClearOnTimeout;

        this.timeoutStateDesc = new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE);
    }

    @Override
    public StateFuture<TriggerResult> onElement(
            T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        return this.nestedTrigger
                .onElement(element, timestamp, window, ctx)
                .thenConditionallyCompose(
                        TriggerResult::isFire,
                        triggerResult -> this.clear(window, ctx).thenApply(ignore -> triggerResult),
                        triggerResult -> {
                            ValueState<Long> timeoutState =
                                    ctx.getPartitionedState(this.timeoutStateDesc);
                            long nextFireTimestamp = ctx.getCurrentProcessingTime() + this.interval;

                            return timeoutState
                                    .asyncValue()
                                    .thenConditionallyCompose(
                                            Objects::nonNull,
                                            timeoutTimestamp -> {
                                                if (resetTimerOnNewRecord) {
                                                    ctx.deleteProcessingTimeTimer(timeoutTimestamp);
                                                    return timeoutState
                                                            .asyncClear()
                                                            .thenApply(ignore -> null);
                                                } else {
                                                    return StateFutureUtils.completedFuture(
                                                            timeoutTimestamp);
                                                }
                                            })
                                    .thenConditionallyCompose(
                                            tuple -> tuple.f1 /*timeoutTimestamp*/ == null,
                                            ignore ->
                                                    timeoutState
                                                            .asyncUpdate(nextFireTimestamp)
                                                            .thenAccept(
                                                                    ignore2 ->
                                                                            ctx
                                                                                    .registerProcessingTimeTimer(
                                                                                            nextFireTimestamp)))
                                    .thenApply(ignore -> triggerResult);
                        })
                .thenApply(tuple -> (TriggerResult) tuple.f1);
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return this.nestedTrigger
                .onProcessingTime(time, window, ctx)
                .thenCompose(
                        triggerResult -> {
                            TriggerResult finalResult =
                                    triggerResult.isPurge()
                                            ? TriggerResult.FIRE_AND_PURGE
                                            : TriggerResult.FIRE;
                            return shouldClearOnTimeout
                                    ? this.clear(window, ctx).thenApply(ignore -> finalResult)
                                    : StateFutureUtils.completedFuture(finalResult);
                        });
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return this.nestedTrigger
                .onEventTime(time, window, ctx)
                .thenCompose(
                        triggerResult -> {
                            TriggerResult finalResult =
                                    triggerResult.isPurge()
                                            ? TriggerResult.FIRE_AND_PURGE
                                            : TriggerResult.FIRE;
                            return shouldClearOnTimeout
                                    ? this.clear(window, ctx).thenApply(ignore -> finalResult)
                                    : StateFutureUtils.completedFuture(finalResult);
                        });
    }

    @Override
    public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
        ValueState<Long> timeoutTimestampState = ctx.getPartitionedState(this.timeoutStateDesc);
        return timeoutTimestampState
                .asyncValue()
                .thenConditionallyCompose(
                        Objects::nonNull,
                        timeoutTimestamp -> {
                            ctx.deleteProcessingTimeTimer(timeoutTimestamp);
                            return timeoutTimestampState.asyncClear();
                        })
                .thenCompose(ignore -> this.nestedTrigger.clear(window, ctx));
    }

    @Override
    public String toString() {
        return "AsyncTimeoutTrigger(" + this.nestedTrigger.toString() + ")";
    }

    /**
     * Creates a new {@link AsyncProcessingTimeoutTrigger} that fires when the inner trigger is
     * fired or when the timeout timer fires.
     *
     * <p>For example: {@code AsyncProcessingTimeoutTrigger.of(AsyncCountTrigger.of(3), 100)}, will
     * create a AsyncCountTrigger with timeout of 100 millis. So, if the first record arrives at
     * time {@code t}, and the second record arrives at time {@code t+50 }, the trigger will fire
     * when the third record arrives or when the time is {code t+100} (timeout).
     *
     * @param nestedTrigger the nested {@link AsyncTrigger}
     * @param timeout the timeout interval
     * @return {@link AsyncProcessingTimeoutTrigger} with the above configuration.
     */
    public static <T, W extends Window> AsyncProcessingTimeoutTrigger<T, W> of(
            AsyncTrigger<T, W> nestedTrigger, Duration timeout) {
        return new AsyncProcessingTimeoutTrigger<>(nestedTrigger, timeout.toMillis(), false, true);
    }

    /**
     * Creates a new {@link AsyncProcessingTimeoutTrigger} that fires when the inner trigger is
     * fired or when the timeout timer fires.
     *
     * <p>For example: {@code AsyncProcessingTimeoutTrigger.of(CountTrigger.of(3), 100, false,
     * true)}, will create a AsyncCountTrigger with timeout of 100 millis. So, if the first record
     * arrives at time {@code t}, and the second record arrives at time {@code t+50 }, the trigger
     * will fire when the third record arrives or when the time is {code t+100} (timeout).
     *
     * @param nestedTrigger the nested {@link AsyncTrigger}
     * @param timeout the timeout interval
     * @param resetTimerOnNewRecord each time a new element arrives, reset the timer and start a new
     *     one
     * @param shouldClearOnTimeout whether to call {@link AsyncTrigger#clear(Window,
     *     AsyncTrigger.TriggerContext)} when the processing-time timer fires
     * @param <T> The type of the element.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     * @return {@link AsyncProcessingTimeoutTrigger} with the above configuration.
     */
    public static <T, W extends Window> AsyncProcessingTimeoutTrigger<T, W> of(
            AsyncTrigger<T, W> nestedTrigger,
            Duration timeout,
            boolean resetTimerOnNewRecord,
            boolean shouldClearOnTimeout) {
        return new AsyncProcessingTimeoutTrigger<>(
                nestedTrigger, timeout.toMillis(), resetTimerOnNewRecord, shouldClearOnTimeout);
    }

    @VisibleForTesting
    public AsyncTrigger<T, W> getNestedTrigger() {
        return nestedTrigger;
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    @VisibleForTesting
    public boolean isResetTimerOnNewRecord() {
        return resetTimerOnNewRecord;
    }

    @VisibleForTesting
    public boolean isShouldClearOnTimeout() {
        return shouldClearOnTimeout;
    }

    @VisibleForTesting
    public ValueStateDescriptor<Long> getTimeoutStateDesc() {
        return timeoutStateDesc;
    }
}
