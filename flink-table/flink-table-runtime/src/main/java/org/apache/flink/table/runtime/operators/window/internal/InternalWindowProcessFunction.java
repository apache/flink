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

package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Collection;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;

/**
 * The internal interface for functions that process over grouped windows.
 *
 * @param <W> type of window
 */
public abstract class InternalWindowProcessFunction<K, W extends Window> implements Serializable {

    private static final long serialVersionUID = 5191040787066951059L;

    protected final WindowAssigner<W> windowAssigner;
    protected final NamespaceAggsHandleFunctionBase<W> windowAggregator;
    protected final long allowedLateness;
    protected Context<K, W> ctx;

    protected InternalWindowProcessFunction(
            WindowAssigner<W> windowAssigner,
            NamespaceAggsHandleFunctionBase<W> windowAggregator,
            long allowedLateness) {
        this.windowAssigner = windowAssigner;
        this.windowAggregator = windowAggregator;
        this.allowedLateness = allowedLateness;
    }

    /** Initialization method for the function. It is called before the actual working methods. */
    public void open(Context<K, W> ctx) throws Exception {
        this.ctx = ctx;
        this.windowAssigner.open(ctx);
    }

    /**
     * Assigns the input element into the state namespace which the input element should be
     * accumulated/retracted into.
     *
     * @param inputRow the input element
     * @param timestamp the timestamp of the element or the processing time (depends on the type of
     *     assigner)
     * @return the state namespace
     */
    public abstract Collection<W> assignStateNamespace(RowData inputRow, long timestamp)
            throws Exception;

    /**
     * Assigns the input element into the actual windows which the {@link Trigger} should trigger
     * on.
     *
     * @param inputRow the input element
     * @param timestamp the timestamp of the element or the processing time (depends on the type of
     *     assigner)
     * @return the actual windows
     */
    public abstract Collection<W> assignActualWindows(RowData inputRow, long timestamp)
            throws Exception;

    /**
     * Prepares the accumulator of the given window before emit the final result. The accumulator is
     * stored in the state or will be created if there is no corresponding accumulator in state.
     *
     * @param window the window
     */
    public abstract void prepareAggregateAccumulatorForEmit(W window) throws Exception;

    /**
     * Cleans the given window if needed.
     *
     * @param window the window to cleanup
     * @param currentTime the current timestamp
     */
    public abstract void cleanWindowIfNeeded(W window, long currentTime) throws Exception;

    /**
     * The tear-down method of the function. It is called after the last call to the main working
     * methods.
     */
    public void close() throws Exception {}

    /** Returns {@code true} if the given time is the cleanup time for the given window. */
    protected final boolean isCleanupTime(W window, long time) {
        return time == toEpochMillsForTimer(cleanupTime(window), ctx.getShiftTimeZone());
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
     * the given window.
     */
    protected boolean isWindowLate(W window) {
        return (windowAssigner.isEventTime()
                && (toEpochMillsForTimer(cleanupTime(window), ctx.getShiftTimeZone())
                        <= ctx.currentWatermark()));
    }

    /**
     * Returns the cleanup time for a window, which is {@code window.maxTimestamp +
     * allowedLateness}. In case this leads to a value greated than {@link Long#MAX_VALUE} then a
     * cleanup time of {@link Long#MAX_VALUE} is returned.
     *
     * @param window the window whose cleanup time we are computing.
     */
    private long cleanupTime(W window) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }

    /**
     * Information available in an invocation of methods of {@link InternalWindowProcessFunction}.
     *
     * @param <W>
     */
    public interface Context<K, W extends Window> {

        /**
         * Creates a partitioned state handle, using the state backend configured for this task.
         *
         * @throws IllegalStateException Thrown, if the key/value state was already initialized.
         * @throws Exception Thrown, if the state backend cannot create the key/value state.
         */
        <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
                throws Exception;

        /** @return current key of current processed element. */
        K currentKey();

        /** Returns the current processing time. */
        long currentProcessingTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /** Returns the shifted timezone of the window. */
        ZoneId getShiftTimeZone();

        /** Gets the accumulators of the given window. */
        RowData getWindowAccumulators(W window) throws Exception;

        /** Sets the accumulators of the given window. */
        void setWindowAccumulators(W window, RowData acc) throws Exception;

        /** Clear window state of the given window. */
        void clearWindowState(W window) throws Exception;

        /** Clear previous agg state (used for retraction) of the given window. */
        void clearPreviousState(W window) throws Exception;

        /** Call {@link Trigger#clear(Window)}} on trigger. */
        void clearTrigger(W window) throws Exception;

        /** Call {@link Trigger#onMerge(Window, Trigger.OnMergeContext)} on trigger. */
        void onMerge(W newWindow, Collection<W> mergedWindows) throws Exception;

        /** Deletes the cleanup timer set for the contents of the provided window. */
        void deleteCleanupTimer(W window) throws Exception;
    }
}
