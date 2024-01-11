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

import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collection;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;

/** A base context for window. */
public abstract class AbstractWindowContext<K, W extends Window> implements WindowContext<K, W> {

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    private final ZoneId shiftTimeZone;

    private final InternalTimerService<W> internalTimerService;

    private final InternalValueState<K, W, RowData> windowState;

    protected @Nullable InternalValueState<K, W, RowData> previousState;

    private final NamespaceAggsHandleFunctionBase<W> windowAggregator;

    private final GroupWindowAssigner<W> windowAssigner;

    private final AbstractTriggerContext<K, W> triggerContext;

    public AbstractWindowContext(
            ZoneId shiftTimeZone,
            InternalTimerService<W> internalTimerService,
            InternalValueState<K, W, RowData> windowState,
            @Nullable InternalValueState<K, W, RowData> previousState,
            NamespaceAggsHandleFunctionBase<W> windowAggregator,
            GroupWindowAssigner<W> windowAssigner,
            AbstractTriggerContext<K, W> triggerContext) {
        this.shiftTimeZone = shiftTimeZone;
        this.internalTimerService = internalTimerService;
        this.windowState = windowState;
        this.previousState = previousState;
        this.windowAggregator = windowAggregator;
        this.windowAssigner = windowAssigner;
        this.triggerContext = triggerContext;
    }

    @Override
    public long currentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return internalTimerService.currentWatermark();
    }

    @Override
    public ZoneId getShiftTimeZone() {
        return shiftTimeZone;
    }

    @Override
    public RowData getWindowAccumulators(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.value();
    }

    @Override
    public void setWindowAccumulators(W window, RowData acc) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.update(acc);
    }

    @Override
    public void clearWindowState(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.clear();
        windowAggregator.cleanup(window);
    }

    @Override
    public void clearPreviousState(W window) throws Exception {
        if (previousState != null) {
            previousState.setCurrentNamespace(window);
            previousState.clear();
        }
    }

    @Override
    public void clearTrigger(W window) throws Exception {
        triggerContext.setWindow(window);
        triggerContext.clear();
    }

    protected abstract long findCleanupTime(W window);

    @Override
    public void deleteCleanupTimer(W window) throws Exception {
        long cleanupTime = toEpochMillsForTimer(findCleanupTime(window), shiftTimeZone);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.deleteEventTimeTimer(cleanupTime);
        } else {
            triggerContext.deleteProcessingTimeTimer(cleanupTime);
        }
    }

    @Override
    public void onMerge(W newWindow, Collection<W> mergedWindows) throws Exception {
        triggerContext.setWindow(newWindow);
        triggerContext.setMergedWindows(mergedWindows);
        triggerContext.onMerge();
    }
}
