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
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link AsyncTrigger} that fires once the watermark passes the end of the window to which a pane
 * belongs. This is for async window operator.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
@Experimental
public class AsyncEventTimeTrigger extends AsyncTrigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private AsyncEventTimeTrigger() {}

    @Override
    public StateFuture<TriggerResult> onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return StateFutureUtils.completedFuture(TriggerResult.FIRE);
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
        }
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(
            long time, TimeWindow window, TriggerContext ctx) {
        return StateFutureUtils.completedFuture(
                time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(
            long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<Void> clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "AsyncEventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static AsyncEventTimeTrigger create() {
        return new AsyncEventTimeTrigger();
    }
}
