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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A trigger that can turn any {@link AsyncTrigger} into a purging {@code Trigger}. This is for
 * async window operator
 *
 * <p>When the nested trigger fires, this will return a {@code FIRE_AND_PURGE} {@link
 * TriggerResult}.
 *
 * @param <T> The type of elements on which this trigger can operate.
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@Experimental
public class AsyncPurgingTrigger<T, W extends Window> extends AsyncTrigger<T, W> {
    private static final long serialVersionUID = 1L;

    private AsyncTrigger<T, W> nestedTrigger;

    private AsyncPurgingTrigger(AsyncTrigger<T, W> nestedTrigger) {
        this.nestedTrigger = nestedTrigger;
    }

    @Override
    public StateFuture<TriggerResult> onElement(
            T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        return nestedTrigger
                .onElement(element, timestamp, window, ctx)
                .thenApply(
                        triggerResult ->
                                triggerResult.isFire()
                                        ? TriggerResult.FIRE_AND_PURGE
                                        : triggerResult);
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return nestedTrigger
                .onEventTime(time, window, ctx)
                .thenApply(
                        triggerResult ->
                                triggerResult.isFire()
                                        ? TriggerResult.FIRE_AND_PURGE
                                        : triggerResult);
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return nestedTrigger
                .onProcessingTime(time, window, ctx)
                .thenApply(
                        triggerResult ->
                                triggerResult.isFire()
                                        ? TriggerResult.FIRE_AND_PURGE
                                        : triggerResult);
    }

    @Override
    public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
        return nestedTrigger.clear(window, ctx);
    }

    @Override
    public boolean canMerge() {
        return nestedTrigger.canMerge();
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        nestedTrigger.onMerge(window, ctx);
    }

    @Override
    public String toString() {
        return "PurgingTrigger(" + nestedTrigger.toString() + ")";
    }

    /**
     * Creates a new purging trigger from the given {@code Trigger}.
     *
     * @param nestedTrigger The trigger that is wrapped by this purging trigger
     */
    public static <T, W extends Window> AsyncPurgingTrigger<T, W> of(
            AsyncTrigger<T, W> nestedTrigger) {
        return new AsyncPurgingTrigger<>(nestedTrigger);
    }

    @VisibleForTesting
    public AsyncTrigger<T, W> getNestedTrigger() {
        return nestedTrigger;
    }
}
