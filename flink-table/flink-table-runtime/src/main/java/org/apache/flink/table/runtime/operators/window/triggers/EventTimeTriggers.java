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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.runtime.operators.window.Window;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Trigger} that reacts to event-time timers. The behavior can be one of the following:
 *
 * <ul>
 *   <li/>fire when the watermark passes the end of the window ({@link
 *       EventTimeTriggers#afterEndOfWindow()}),
 * </ul>
 *
 * <p>In the first case, the trigger can also specify an <tt>early</tt> and a <tt>late</tt> trigger.
 * The <tt>early trigger</tt> will be responsible for specifying when the trigger should fire in the
 * period between the beginning of the window and the time when the watermark passes the end of the
 * window. The <tt>late trigger</tt> takes over after the watermark passes the end of the window,
 * and specifies when the trigger should fire in the period between the <tt>endOfWindow</tt> and
 * <tt>endOfWindow + allowedLateness</tt>.
 */
public class EventTimeTriggers {

    private static final String TO_STRING = "EventTime.afterEndOfWindow()";

    /** This class should never be instantiated. */
    private EventTimeTriggers() {}

    /** Creates a trigger that fires when the watermark passes the end of the window. */
    public static <W extends Window> AfterEndOfWindow<W> afterEndOfWindow() {
        return new AfterEndOfWindow<>();
    }

    /**
     * A {@link Trigger} that fires once the watermark passes the end of the window to which a pane
     * belongs.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    public static final class AfterEndOfWindow<W extends Window> extends WindowTrigger<W> {

        private static final long serialVersionUID = -6379468077823588591L;

        /**
         * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
         * given {@code Trigger} fires before the watermark has passed the end of the window.
         */
        public AfterEndOfWindowNoLate<W> withEarlyFirings(Trigger<W> earlyFirings) {
            checkNotNull(earlyFirings);
            return new AfterEndOfWindowNoLate<>(earlyFirings);
        }

        /**
         * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
         * given {@code Trigger} fires after the watermark has passed the end of the window.
         */
        public Trigger<W> withLateFirings(Trigger<W> lateFirings) {
            checkNotNull(lateFirings);
            if (lateFirings instanceof ElementTriggers.EveryElement) {
                // every-element late firing can be ignored
                return this;
            } else {
                return new AfterEndOfWindowEarlyAndLate<>(null, lateFirings);
            }
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            if (triggerTime(window) <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return true;
            } else {
                ctx.registerEventTimeTimer(triggerTime(window));
                return false;
            }
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return time == triggerTime(window);
        }

        @Override
        public void clear(W window) throws Exception {
            ctx.deleteEventTimeTimer(triggerTime(window));
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            ctx.registerEventTimeTimer(triggerTime(window));
        }

        @Override
        public String toString() {
            return TO_STRING;
        }
    }

    /**
     * A composite {@link Trigger} that consist of AfterEndOfWindow and a early trigger and late
     * trigger.
     */
    public static final class AfterEndOfWindowEarlyAndLate<W extends Window>
            extends WindowTrigger<W> {

        private static final long serialVersionUID = -800582945577030338L;

        private final Trigger<W> earlyTrigger;
        private final Trigger<W> lateTrigger;
        private final ValueStateDescriptor<Boolean> hasFiredOnTimeStateDesc;

        AfterEndOfWindowEarlyAndLate(Trigger<W> earlyTrigger, Trigger<W> lateTrigger) {
            this.earlyTrigger = earlyTrigger;
            this.lateTrigger = lateTrigger;
            this.hasFiredOnTimeStateDesc =
                    new ValueStateDescriptor<>("eventTime-afterEOW", Types.BOOLEAN);
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
            if (earlyTrigger != null) {
                earlyTrigger.open(ctx);
            }
            if (lateTrigger != null) {
                lateTrigger.open(ctx);
            }
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            Boolean hasFired = ctx.getPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired != null && hasFired) {
                // this is to cover the case where we recover from a failure and the watermark
                // is Long.MIN_VALUE but the window is already in the late phase.
                return lateTrigger != null && lateTrigger.onElement(element, timestamp, window);
            } else {
                if (triggerTime(window) <= ctx.getCurrentWatermark()) {
                    // we are in the late phase

                    // if there is no late trigger then we fire on every late element
                    // This also covers the case of recovery after a failure
                    // where the currentWatermark will be Long.MIN_VALUE
                    return true;
                } else {
                    // we are in the early phase
                    ctx.registerEventTimeTimer(triggerTime(window));
                    return earlyTrigger != null
                            && earlyTrigger.onElement(element, timestamp, window);
                }
            }
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            Boolean hasFired = ctx.getPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired != null && hasFired) {
                // late fire
                return lateTrigger != null && lateTrigger.onProcessingTime(time, window);
            } else {
                // early fire
                return earlyTrigger != null && earlyTrigger.onProcessingTime(time, window);
            }
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            ValueState<Boolean> hasFiredState = ctx.getPartitionedState(hasFiredOnTimeStateDesc);
            Boolean hasFired = hasFiredState.value();
            if (hasFired != null && hasFired) {
                // late fire
                return lateTrigger != null && lateTrigger.onEventTime(time, window);
            } else {
                if (time == triggerTime(window)) {
                    // fire on time and update state
                    hasFiredState.update(true);
                    return true;
                } else {
                    // early fire
                    return earlyTrigger != null && earlyTrigger.onEventTime(time, window);
                }
            }
        }

        @Override
        public boolean canMerge() {
            return (earlyTrigger == null || earlyTrigger.canMerge())
                    && (lateTrigger == null || lateTrigger.canMerge());
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            if (earlyTrigger != null) {
                earlyTrigger.onMerge(window, mergeContext);
            }
            if (lateTrigger != null) {
                lateTrigger.onMerge(window, mergeContext);
            }

            // we assume that the new merged window has not fired yet its on-time timer.
            ctx.getPartitionedState(hasFiredOnTimeStateDesc).update(false);

            ctx.registerEventTimeTimer(triggerTime(window));
        }

        @Override
        public void clear(W window) throws Exception {
            if (earlyTrigger != null) {
                earlyTrigger.clear(window);
            }
            if (lateTrigger != null) {
                lateTrigger.clear(window);
            }
            ctx.deleteEventTimeTimer(triggerTime(window));
            ctx.getPartitionedState(hasFiredOnTimeStateDesc).clear();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(TO_STRING);
            if (earlyTrigger != null) {
                builder.append(".withEarlyFirings(").append(earlyTrigger).append(")");
            }
            if (lateTrigger != null) {
                builder.append(".withLateFirings(").append(lateTrigger).append(")");
            }
            return builder.toString();
        }
    }

    /** A composite {@link Trigger} that consist of AfterEndOfWindow and a late trigger. */
    public static final class AfterEndOfWindowNoLate<W extends Window> extends WindowTrigger<W> {

        private static final long serialVersionUID = -4334481808648361926L;

        /**
         * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever the
         * given {@code Trigger} fires after the watermark has passed the end of the window.
         */
        public Trigger<W> withLateFirings(Trigger<W> lateFirings) {
            checkNotNull(lateFirings);
            if (lateFirings instanceof ElementTriggers.EveryElement) {
                // every-element late firing can be ignored
                return this;
            } else {
                return new AfterEndOfWindowEarlyAndLate<>(earlyTrigger, lateFirings);
            }
        }

        // early trigger is always not null
        private final Trigger<W> earlyTrigger;

        private AfterEndOfWindowNoLate(Trigger<W> earlyTrigger) {
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
            if (triggerTime(window) <= ctx.getCurrentWatermark()) {
                // the on-time firing
                return true;
            } else {
                // this is an early element so register the timer and let the early trigger decide
                ctx.registerEventTimeTimer(triggerTime(window));
                return earlyTrigger.onElement(element, timestamp, window);
            }
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return earlyTrigger.onProcessingTime(time, window);
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return time == triggerTime(window) || earlyTrigger.onEventTime(time, window);
        }

        @Override
        public boolean canMerge() {
            return earlyTrigger.canMerge();
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            ctx.registerEventTimeTimer(triggerTime(window));
            earlyTrigger.onMerge(window, mergeContext);
        }

        @Override
        public void clear(W window) throws Exception {
            ctx.deleteEventTimeTimer(triggerTime(window));
            earlyTrigger.clear(window);
        }

        @Override
        public String toString() {
            return TO_STRING + ".withEarlyFirings(" + earlyTrigger + ")";
        }
    }
}
