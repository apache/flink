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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into sessions based on the current processing
 * time. Windows cannot overlap.
 *
 * <p>For example, in order to window into windows with a dynamic time gap:
 *
 * <pre>{@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(DynamicProcessingTimeSessionWindows.withDynamicGap({@link SessionWindowTimeGapExtractor }));
 * }</pre>
 *
 * @param <T> The type of the input elements
 */
@PublicEvolving
public class DynamicProcessingTimeSessionWindows<T> extends MergingWindowAssigner<T, TimeWindow> {
    private static final long serialVersionUID = 1L;

    protected SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor;

    protected DynamicProcessingTimeSessionWindows(
            SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor) {
        this.sessionWindowTimeGapExtractor = sessionWindowTimeGapExtractor;
    }

    @Override
    public Collection<TimeWindow> assignWindows(
            T element, long timestamp, WindowAssignerContext context) {
        long currentProcessingTime = context.getCurrentProcessingTime();
        long sessionTimeout = sessionWindowTimeGapExtractor.extract(element);
        if (sessionTimeout <= 0) {
            throw new IllegalArgumentException("Dynamic session time gap must satisfy 0 < gap");
        }
        return Collections.singletonList(
                new TimeWindow(currentProcessingTime, currentProcessingTime + sessionTimeout));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Trigger<T, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return (Trigger<T, TimeWindow>) ProcessingTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "DynamicProcessingTimeSessionWindows()";
    }

    /**
     * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns elements to sessions
     * based on the element timestamp.
     *
     * @param sessionWindowTimeGapExtractor The extractor to use to extract the time gap from the
     *     input elements
     * @return The policy.
     */
    public static <T> DynamicProcessingTimeSessionWindows<T> withDynamicGap(
            SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor) {
        return new DynamicProcessingTimeSessionWindows<>(sessionWindowTimeGapExtractor);
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    /** Merge overlapping {@link TimeWindow}s. */
    @Override
    public void mergeWindows(Collection<TimeWindow> windows, MergeCallback<TimeWindow> c) {
        TimeWindow.mergeWindows(windows, c);
    }
}
