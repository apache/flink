/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;

/**
 * A {@link AsyncTrigger} that continuously fires based on a given time interval. This fires based
 * on {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks}.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
@Internal
public class AsyncContinuousEventTimeTrigger<W extends Window> extends AsyncTrigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long interval;

    /** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private AsyncContinuousEventTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public StateFuture<TriggerResult> onElement(
            Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return StateFutureUtils.completedFuture(TriggerResult.FIRE);
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
        return fireTimestampState
                .asyncGet()
                .thenCompose(
                        ts -> {
                            if (ts == null) {
                                registerNextFireTimestamp(
                                        timestamp - (timestamp % interval),
                                        window,
                                        ctx,
                                        fireTimestampState);
                            }

                            return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
                        });
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx)
            throws Exception {

        if (time == window.maxTimestamp()) {
            return StateFutureUtils.completedFuture(TriggerResult.FIRE);
        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
        return fireTimestampState
                .asyncGet()
                .thenCompose(
                        fireTimestamp -> {
                            if (fireTimestamp != null && fireTimestamp == time) {
                                return fireTimestampState
                                        .asyncClear()
                                        .thenCompose(
                                                (ignore) ->
                                                        registerNextFireTimestamp(
                                                                time,
                                                                window,
                                                                ctx,
                                                                fireTimestampState))
                                        .thenApply(ignore -> TriggerResult.FIRE);
                            }

                            return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
                        });
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        return fireTimestamp
                .asyncGet()
                .thenCompose(
                        ts -> {
                            if (ts != null) {
                                ctx.deleteEventTimeTimer(ts);
                                return fireTimestamp.asyncClear();
                            } else {
                                return StateFutureUtils.completedVoidFuture();
                            }
                        });
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        throw new RuntimeException("Merge window not support");
    }

    @Override
    public String toString() {
        return "ContinuousEventTimeTrigger(" + interval + ")";
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    /**
     * Creates a trigger that continuously fires based on the given interval.
     *
     * @param interval The time interval at which to fire.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <W extends Window> AsyncContinuousEventTimeTrigger<W> of(Duration interval) {
        return new AsyncContinuousEventTimeTrigger<>(interval.toMillis());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private StateFuture<Void> registerNextFireTimestamp(
            long time, W window, TriggerContext ctx, ReducingState<Long> fireTimestampState)
            throws Exception {
        long nextFireTimestamp = Math.min(time + interval, window.maxTimestamp());
        return fireTimestampState
                .asyncAdd(nextFireTimestamp)
                .thenAccept(ignore -> ctx.registerEventTimeTimer(nextFireTimestamp));
    }
}
