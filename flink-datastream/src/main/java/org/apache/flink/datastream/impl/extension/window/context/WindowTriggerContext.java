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

package org.apache.flink.datastream.impl.extension.window.context;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

/**
 * {@code WindowTriggerContext} is a utility for handling {@code Trigger} invocations. It can be
 * reused by setting the {@code key} and {@code window} fields.
 *
 * @param <K> Type of the key.
 * @param <IN> Type of the input value.
 * @param <W> Type of the window.
 */
public class WindowTriggerContext<K, IN, W extends Window> implements Trigger.OnMergeContext {

    /** Current processing key. */
    private K key;

    /** Current processing window. */
    private W window;

    /** The operator to which the window belongs, used for creating and retrieving window state. */
    private final AbstractStreamOperator<?> operator;

    /** The timer service of {@code operator}. */
    private final InternalTimerService<W> internalTimerService;

    private final TypeSerializer<W> windowSerializer;

    private final Trigger<? super IN, ? super W> trigger;

    private Collection<W> mergedWindows;

    public WindowTriggerContext(
            K key,
            W window,
            AbstractStreamOperator<?> operator,
            InternalTimerService<W> internalTimerService,
            Trigger<? super IN, ? super W> trigger,
            TypeSerializer<W> windowSerializer) {
        this.key = key;
        this.window = window;
        this.operator = operator;
        this.internalTimerService = internalTimerService;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return operator.getMetricGroup();
    }

    /** Get current event time. */
    public long getCurrentWatermark() {
        return internalTimerService.currentWatermark();
    }

    public <S extends State> S getPartitionedState(
            org.apache.flink.api.common.state.StateDescriptor<S, ?> stateDescriptor) {
        try {
            return operator.getPartitionedState(window, windowSerializer, stateDescriptor);
        } catch (Exception e) {
            throw new RuntimeException("Could not retrieve state", e);
        }
    }

    @Override
    public <S extends MergingState<?, ?>> void mergePartitionedState(
            org.apache.flink.api.common.state.StateDescriptor<S, ?> stateDescriptor) {
        if (mergedWindows != null && !mergedWindows.isEmpty()) {
            try {
                S rawState =
                        operator.getKeyedStateBackend()
                                .getOrCreateKeyedState(windowSerializer, stateDescriptor);

                if (rawState
                        instanceof org.apache.flink.runtime.state.internal.InternalMergingState) {
                    @SuppressWarnings("unchecked")
                    org.apache.flink.runtime.state.internal.InternalMergingState<K, W, ?, ?, ?>
                            mergingState =
                                    (org.apache.flink.runtime.state.internal.InternalMergingState<
                                                    K, W, ?, ?, ?>)
                                            rawState;
                    mergingState.mergeNamespaces(window, mergedWindows);
                } else {
                    throw new IllegalArgumentException(
                            "The given state descriptor does not refer to a mergeable state (MergingState)");
                }
            } catch (Exception e) {
                throw new RuntimeException("Error while merging state.", e);
            }
        }
    }

    @Override
    public long getCurrentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public void registerProcessingTimeTimer(long time) {
        internalTimerService.registerProcessingTimeTimer(window, time);
    }

    @Override
    public void registerEventTimeTimer(long time) {
        internalTimerService.registerEventTimeTimer(window, time);
    }

    @Override
    public void deleteProcessingTimeTimer(long time) {
        internalTimerService.deleteProcessingTimeTimer(window, time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        internalTimerService.deleteEventTimeTimer(window, time);
    }

    public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
        return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
    }

    public TriggerResult onProcessingTime(long time) throws Exception {
        return trigger.onProcessingTime(time, window, this);
    }

    public TriggerResult onEventTime(long time) throws Exception {
        return trigger.onEventTime(time, window, this);
    }

    public void onMerge(Collection<W> mergedWindows) throws Exception {
        this.mergedWindows = mergedWindows;
        trigger.onMerge(window, this);
    }

    public void clear() throws Exception {
        trigger.clear(window, this);
    }

    @Override
    public String toString() {
        return "WindowTriggerContext{" + "key=" + key + ", window=" + window + '}';
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setWindow(W window) {
        this.window = window;
    }

    public K getKey() {
        return key;
    }

    public W getWindow() {
        return window;
    }
}
