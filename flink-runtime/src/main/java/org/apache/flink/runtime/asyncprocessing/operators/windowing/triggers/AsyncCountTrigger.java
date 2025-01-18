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

package org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link AsyncTrigger} that fires once the count of elements in a pane reaches the given count.
 * This is for async window operator.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@Experimental
public class AsyncCountTrigger<W extends Window> extends AsyncTrigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    private AsyncCountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public StateFuture<TriggerResult> onElement(
            Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);

        return count.asyncAdd(1L)
                .thenCompose(
                        ignore -> {
                            return count.asyncGet();
                        })
                .thenCompose(
                        cnt -> {
                            return cnt >= maxCount
                                    ? count.asyncClear()
                                            .thenCompose(
                                                    ignore ->
                                                            StateFutureUtils.completedFuture(
                                                                    TriggerResult.FIRE))
                                    : StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
                        });
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx) {
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
        return ctx.getPartitionedState(stateDesc).asyncClear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateDesc);
    }

    @Override
    public String toString() {
        return "CountTrigger(" + maxCount + ")";
    }

    /**
     * Creates a trigger that fires once the number of elements in a pane reaches the given count.
     *
     * @param maxCount The count of elements at which to fire.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <W extends Window> AsyncCountTrigger<W> of(long maxCount) {
        return new AsyncCountTrigger<>(maxCount);
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
