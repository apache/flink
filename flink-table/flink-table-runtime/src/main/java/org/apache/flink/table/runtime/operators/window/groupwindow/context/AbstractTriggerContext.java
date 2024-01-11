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

package org.apache.flink.table.runtime.operators.window.groupwindow.context;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;

import java.time.ZoneId;
import java.util.Collection;

/**
 * A base context for window trigger.
 *
 * <p>{@link AbstractTriggerContext} is a utility for handling {@link Trigger} invocations. It can
 * be reused by setting the {@code key} and {@code window} fields. No internal state must be kept in
 * the {@link TriggerContext}
 */
public abstract class AbstractTriggerContext<K, W extends Window> implements OnMergeContext {

    private final Trigger<W> trigger;

    private final InternalTimerService<W> internalTimerService;

    private final ZoneId shiftTimeZone;

    private final TypeSerializer<W> windowSerializer;

    protected W window;
    private Collection<W> mergedWindows;

    public AbstractTriggerContext(
            Trigger<W> trigger,
            InternalTimerService<W> internalTimerService,
            ZoneId shiftTimeZone,
            TypeSerializer<W> windowSerializer) {
        this.trigger = trigger;
        this.internalTimerService = internalTimerService;
        this.shiftTimeZone = shiftTimeZone;
        this.windowSerializer = windowSerializer;
    }

    public void open() throws Exception {
        trigger.open(this);
    }

    public boolean onElement(RowData row, long timestamp) throws Exception {
        return trigger.onElement(row, timestamp, window);
    }

    public boolean onProcessingTime(long time) throws Exception {
        return trigger.onProcessingTime(time, window);
    }

    public boolean onEventTime(long time) throws Exception {
        return trigger.onEventTime(time, window);
    }

    public void onMerge() throws Exception {
        trigger.onMerge(window, this);
    }

    public void setWindow(W window) {
        this.window = window;
    }

    public void setMergedWindows(Collection<W> mergedWindows) {
        this.mergedWindows = mergedWindows;
    }

    public W getWindow() {
        return window;
    }

    public Collection<W> getMergedWindows() {
        return mergedWindows;
    }

    @Override
    public long getCurrentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public long getCurrentWatermark() {
        return internalTimerService.currentWatermark();
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

    @Override
    public ZoneId getShiftTimeZone() {
        return shiftTimeZone;
    }

    public void clear() throws Exception {
        trigger.clear(window);
    }

    protected abstract <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception;

    @Override
    public <S extends MergingState<?, ?>> void mergePartitionedState(
            StateDescriptor<S, ?> stateDescriptor) {
        if (mergedWindows != null && mergedWindows.size() > 0) {
            try {
                State state = getOrCreateKeyedState(windowSerializer, stateDescriptor);
                if (state instanceof InternalMergingState) {
                    ((InternalMergingState<K, W, ?, ?, ?>) state)
                            .mergeNamespaces(window, mergedWindows);
                } else {
                    throw new IllegalArgumentException(
                            "The given state descriptor does not refer to a mergeable state (MergingState)");
                }
            } catch (Exception e) {
                throw new RuntimeException("Error while merging state.", e);
            }
        }
    }
}
