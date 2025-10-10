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

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@code AsyncTriggerConverter}. */
public class AsyncTriggerConverterTest {
    private static class DummyTriggerWithoutAsyncConverter extends Trigger<Object, TimeWindow>
            implements AsyncTriggerConverter {
        @Override
        public TriggerResult onElement(
                Object element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return null;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
    }

    private static class DummyTriggerWithAsyncConverter extends DummyTriggerWithoutAsyncConverter {
        @Override
        @Nonnull
        public Object convertToAsync() {
            return new DummyAsyncTrigger();
        }
    }

    private static class DummyAsyncTrigger extends AsyncTrigger<Object, TimeWindow> {
        @Override
        public StateFuture<TriggerResult> onElement(
                Object element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return null;
        }

        @Override
        public StateFuture<TriggerResult> onProcessingTime(
                long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public StateFuture<TriggerResult> onEventTime(
                long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public StateFuture<Void> clear(TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }
    }

    @Test
    void testTriggerUseDefaultConvert() {
        Trigger<Object, TimeWindow> syncTrigger = new DummyTriggerWithoutAsyncConverter();
        AsyncTrigger<Object, TimeWindow> asyncTrigger =
                AsyncTriggerConverter.convertToAsync(syncTrigger);

        assertThat(asyncTrigger).isInstanceOf(AsyncTriggerConverter.UserDefinedAsyncTrigger.class);
        AsyncTriggerConverter.UserDefinedAsyncTrigger<Object, TimeWindow> triggerWrapper =
                (AsyncTriggerConverter.UserDefinedAsyncTrigger<Object, TimeWindow>) asyncTrigger;

        assertThat(triggerWrapper.getUserDefinedTrigger()).isSameAs(syncTrigger);
    }

    @Test
    void testTriggerUseCustomizeConvert() {
        Trigger<Object, TimeWindow> syncTrigger = new DummyTriggerWithAsyncConverter();
        AsyncTrigger<Object, TimeWindow> asyncTrigger =
                AsyncTriggerConverter.convertToAsync(syncTrigger);

        assertThat(asyncTrigger).isInstanceOf(DummyAsyncTrigger.class);
    }
}
