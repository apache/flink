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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;

import java.time.ZoneId;
import java.util.Collection;

/** A context contains some information used for {@link InternalWindowProcessFunction}. */
public interface WindowContext<K, W extends Window> {

    /**
     * Creates a partitioned state handle, using the state backend configured for this task.
     *
     * @throws IllegalStateException Thrown, if the key/value state was already initialized.
     * @throws Exception Thrown, if the state backend cannot create the key/value state.
     */
    <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception;

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

    /** Call {@link Trigger#onMerge(Window, OnMergeContext)} on trigger. */
    void onMerge(W newWindow, Collection<W> mergedWindows) throws Exception;

    /** Deletes the cleanup timer set for the contents of the provided window. */
    void deleteCleanupTimer(W window) throws Exception;
}
