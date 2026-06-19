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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link Trigger} that fires once the current system time passes the end of the window to which a
 * pane belongs. This is for async window operator
 */
@Experimental
public class AsyncProcessingTimeTrigger extends AsyncTrigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private AsyncProcessingTimeTrigger() {}

    @Override
    public StateFuture<TriggerResult> onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<TriggerResult> onEventTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return StateFutureUtils.completedFuture(TriggerResult.CONTINUE);
    }

    @Override
    public StateFuture<TriggerResult> onProcessingTime(
            long time, TimeWindow window, TriggerContext ctx) {
        return StateFutureUtils.completedFuture(TriggerResult.FIRE);
    }

    @Override
    public StateFuture<Void> clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the time is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the time is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ProcessingTimeTrigger()";
    }

    /** Creates a new trigger that fires once system time passes the end of the window. */
    public static AsyncProcessingTimeTrigger create() {
        return new AsyncProcessingTimeTrigger();
    }
}
