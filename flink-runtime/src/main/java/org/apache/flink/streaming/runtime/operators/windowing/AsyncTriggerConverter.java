/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncCountTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncEventTimeTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncProcessingTimeTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncPurgingTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import javax.annotation.Nonnull;

/**
 * A converter from {@code Trigger} to {@code AsyncTrigger}.
 *
 * <p>Basic triggers (e.g., {@code CountTrigger}) are directly converted to their async version.
 *
 * <p>Async-support triggers which implement {@code AsyncTriggerConvertable} (e.g., {@code
 * ProcessingTimeoutTrigger}) will use self-defined async version.
 *
 * <p>Other triggers are wrapped as an {@code AsyncTrigger}, whose internal functions are executed
 * in sync mode.
 */
@Internal
public interface AsyncTriggerConverter {

    /**
     * Convert to an {@code AsyncTrigger}. The default implementation is only a wrapper of the
     * trigger, whose behaviours are all sync.
     *
     * <p>TODO: Return {@code AsyncTrigger} if {@code AsyncTrigger} becomes @PublicEvolving.
     *
     * @return The {@code AsyncTrigger} for async state processing.
     */
    @Nonnull
    default Object convertToAsync() {
        return UserDefinedAsyncTrigger.of((Trigger<?, ?>) AsyncTriggerConverter.this);
    }

    @SuppressWarnings("unchecked")
    static <T, W extends Window> AsyncTrigger<T, W> convertToAsync(Trigger<T, W> trigger) {
        if (trigger instanceof CountTrigger) {
            return (AsyncTrigger<T, W>)
                    AsyncCountTrigger.of(((CountTrigger<?>) trigger).getMaxCount());
        } else if (trigger instanceof EventTimeTrigger) {
            return (AsyncTrigger<T, W>) AsyncEventTimeTrigger.create();
        } else if (trigger instanceof ProcessingTimeTrigger) {
            return (AsyncTrigger<T, W>) AsyncProcessingTimeTrigger.create();
        } else if (trigger instanceof PurgingTrigger) {
            return (AsyncTrigger<T, W>)
                    AsyncPurgingTrigger.of(
                            convertToAsync(((PurgingTrigger<?, ?>) trigger).getNestedTrigger()));
        } else if (trigger instanceof AsyncTriggerConverter) {
            return (AsyncTrigger<T, W>) ((AsyncTriggerConverter) trigger).convertToAsync();
        } else {
            return UserDefinedAsyncTrigger.of(trigger);
        }
    }

    /** Convert non-support user-defined trigger to {@code AsyncTrigger}. */
    class UserDefinedAsyncTrigger<T, W extends Window> extends AsyncTrigger<T, W> {
        private final Trigger<T, W> userDefinedTrigger;

        private UserDefinedAsyncTrigger(Trigger<T, W> userDefinedTrigger) {
            this.userDefinedTrigger = userDefinedTrigger;
        }

        @Override
        public StateFuture<TriggerResult> onElement(
                T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onElement(
                            element, timestamp, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
                throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onProcessingTime(
                            time, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx)
                throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onEventTime(
                            time, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
            userDefinedTrigger.clear(window, AsyncTriggerContextConvertor.of(ctx));
            return StateFutureUtils.completedVoidFuture();
        }

        @Override
        public boolean isEndOfStreamTrigger() {
            return userDefinedTrigger instanceof GlobalWindows.EndOfStreamTrigger;
        }

        public static <T, W extends Window> AsyncTrigger<T, W> of(
                Trigger<T, W> userDefinedTrigger) {
            return new UserDefinedAsyncTrigger<>(userDefinedTrigger);
        }

        /**
         * A converter from {@link AsyncTrigger.TriggerContext} to {@link Trigger.TriggerContext}.
         */
        private static class AsyncTriggerContextConvertor implements Trigger.TriggerContext {

            private final AsyncTrigger.TriggerContext asyncTriggerContext;

            private AsyncTriggerContextConvertor(AsyncTrigger.TriggerContext asyncTriggerContext) {
                this.asyncTriggerContext = asyncTriggerContext;
            }

            @Override
            public long getCurrentProcessingTime() {
                return asyncTriggerContext.getCurrentProcessingTime();
            }

            @Override
            public MetricGroup getMetricGroup() {
                return asyncTriggerContext.getMetricGroup();
            }

            @Override
            public long getCurrentWatermark() {
                return asyncTriggerContext.getCurrentWatermark();
            }

            @Override
            public void registerProcessingTimeTimer(long time) {
                asyncTriggerContext.registerProcessingTimeTimer(time);
            }

            @Override
            public void registerEventTimeTimer(long time) {
                asyncTriggerContext.registerEventTimeTimer(time);
            }

            @Override
            public void deleteProcessingTimeTimer(long time) {
                asyncTriggerContext.deleteProcessingTimeTimer(time);
            }

            @Override
            public void deleteEventTimeTimer(long time) {
                asyncTriggerContext.deleteEventTimeTimer(time);
            }

            @Override
            public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
                throw new UnsupportedOperationException(
                        "Trigger is for state V1 APIs, window operator with async state enabled only accept state V2 APIs.");
            }

            public static Trigger.TriggerContext of(
                    AsyncTrigger.TriggerContext asyncTriggerContext) {
                return new AsyncTriggerContextConvertor(asyncTriggerContext);
            }
        }

        @VisibleForTesting
        public Trigger<T, W> getUserDefinedTrigger() {
            return userDefinedTrigger;
        }
    }
}
