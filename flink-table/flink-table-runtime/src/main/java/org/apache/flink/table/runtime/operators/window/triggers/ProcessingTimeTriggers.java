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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.runtime.operators.window.Window;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Trigger} that reacts to processing-time timers. The behavior can be one of the
 * following:
 *
 * <ul>
 *   <li>fire when the processing time passes the end of the window ({@link
 *       ProcessingTimeTriggers#afterEndOfWindow()}),
 *   <li>fire when the processing time advances by a certain interval after reception of the first
 *       element after the last firing for a given window ({@link
 *       ProcessingTimeTriggers#every(Duration)}).
 * </ul>
 *
 * <p>In the first case, the trigger can also specify an <tt>early</tt> trigger. The <tt>early
 * trigger</tt> will be responsible for specifying when the trigger should fire in the period
 * between the beginning of the window and the time when the processing time passes the end of the
 * window.
 */
public class ProcessingTimeTriggers {

    private static final String TO_STRING = "ProcessingTime.afterEndOfWindow()";

    /** This class should never be instantiated. */
    private ProcessingTimeTriggers() {}

    /** Creates a trigger that fires when the processing time passes the end of the window. */
    public static <W extends Window> AfterEndOfWindow<W> afterEndOfWindow() {
        return new AfterEndOfWindow<>();
    }

    /**
     * Creates a trigger that fires by a certain interval after reception of the first element.
     *
     * @param time the certain interval
     */
    public static <W extends Window> AfterFirstElementPeriodic<W> every(Duration time) {
        return new AfterFirstElementPeriodic<>(time.toMillis());
    }

    /**
     * Trigger every a given interval, the first trigger time is interval after the first element in
     * the pane.
     *
     * @param <W> type of window
     */
    public static final class AfterFirstElementPeriodic<W extends Window> extends WindowTrigger<W> {

        private static final long serialVersionUID = -4710472821577125673L;

        private final long interval;
        private final ReducingStateDescriptor<Long> nextFiringStateDesc;

        AfterFirstElementPeriodic(long interval) {
            checkArgument(interval > 0);
            this.interval = interval;
            this.nextFiringStateDesc =
                    new ReducingStateDescriptor<>(
                            "processingTime-every-" + interval, new Min(), LongSerializer.INSTANCE);
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            ReducingState<Long> nextFiring = ctx.getPartitionedState(nextFiringStateDesc);
            if (nextFiring.get() == null) {
                long nextTimer = ctx.getCurrentProcessingTime() + interval;
                ctx.registerProcessingTimeTimer(nextTimer);
                nextFiring.add(nextTimer);
            }
            return false;
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            ReducingState<Long> nextFiring = ctx.getPartitionedState(nextFiringStateDesc);
            Long timer = nextFiring.get();
            if (timer != null && timer == time) {
                long newTimer = time + interval;
                ctx.registerProcessingTimeTimer(newTimer);
                nextFiring.clear();
                nextFiring.add(newTimer);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            mergeContext.mergePartitionedState(nextFiringStateDesc);

            // after merge, the merged state will be stored in current window
            Long nextTimer = ctx.getPartitionedState(nextFiringStateDesc).get();
            if (nextTimer != null) {
                ctx.registerProcessingTimeTimer(nextTimer);
            }
        }

        @Override
        public void clear(W window) throws Exception {
            ReducingState<Long> nextFiring = ctx.getPartitionedState(nextFiringStateDesc);
            Long timer = nextFiring.get();
            if (timer != null) {
                ctx.deleteProcessingTimeTimer(timer);
                nextFiring.clear();
            }
        }

        @Override
        public String toString() {
            return "ProcessingTime.every(" + interval + ")";
        }

        private static class Min implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return Math.min(value1, value2);
            }
        }
    }

    /**
     * A {@link Trigger} that fires once the current system time passes the end of the window to
     * which a pane belongs.
     */
    public static final class AfterEndOfWindow<W extends Window> extends WindowTrigger<W> {
        private static final long serialVersionUID = 2369815941792574642L;

        /**
         * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
         * given {@code Trigger} fires before the processing time has passed the end of the window.
         */
        public AfterEndOfWindowNoLate<W> withEarlyFirings(Trigger<W> earlyFirings) {
            checkNotNull(earlyFirings);
            return new AfterEndOfWindowNoLate<>(earlyFirings);
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            ctx.registerProcessingTimeTimer(triggerTime(window));
            return false;
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return time == triggerTime(window);
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public void clear(W window) throws Exception {
            ctx.deleteProcessingTimeTimer(triggerTime(window));
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            ctx.registerProcessingTimeTimer(triggerTime(window));
        }

        @Override
        public String toString() {
            return TO_STRING;
        }
    }

    /** A composite {@link Trigger} that consist of AfterEndOfWindow and a early trigger. */
    public static final class AfterEndOfWindowNoLate<W extends Window> extends WindowTrigger<W> {

        private static final long serialVersionUID = 2310050856564792734L;

        // early trigger is always not null
        private final Trigger<W> earlyTrigger;

        AfterEndOfWindowNoLate(Trigger<W> earlyTrigger) {
            checkNotNull(earlyTrigger);
            this.earlyTrigger = earlyTrigger;
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
            earlyTrigger.open(ctx);
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            ctx.registerProcessingTimeTimer(triggerTime(window));
            return earlyTrigger.onElement(element, timestamp, window);
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return time == triggerTime(window) || earlyTrigger.onProcessingTime(time, window);
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return earlyTrigger.onEventTime(time, window);
        }

        @Override
        public boolean canMerge() {
            return earlyTrigger.canMerge();
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            ctx.registerProcessingTimeTimer(triggerTime(window));
            earlyTrigger.onMerge(window, mergeContext);
        }

        @Override
        public void clear(W window) throws Exception {
            ctx.deleteProcessingTimeTimer(triggerTime(window));
            earlyTrigger.clear(window);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(TO_STRING);
            if (earlyTrigger != null) {
                builder.append(".withEarlyFirings(").append(earlyTrigger).append(")");
            }
            return builder.toString();
        }
    }
}
