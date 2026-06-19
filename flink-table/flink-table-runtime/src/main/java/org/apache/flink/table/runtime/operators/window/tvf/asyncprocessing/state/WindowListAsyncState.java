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

package org.apache.flink.table.runtime.operators.window.tvf.asyncprocessing.state;

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.table.data.RowData;

/** A wrapper of {@link ListState} which is easier to update based on window namespace. */
public final class WindowListAsyncState<W> implements WindowAsyncState<W> {

    private final InternalListState<RowData, W, RowData> windowState;

    public WindowListAsyncState(InternalListState<RowData, W, RowData> windowState) {
        this.windowState = windowState;
    }

    @Override
    public StateFuture<Void> asyncClear(W window) {
        windowState.setCurrentNamespace(window);
        return windowState.asyncClear();
    }

    public StateFuture<StateIterator<RowData>> asyncGet(W window) {
        windowState.setCurrentNamespace(window);
        return windowState.asyncGet();
    }

    /**
     * Updates the operator state accessible by {@link #asyncGet(W)} by adding the given value to
     * the list of values. The next time {@link #asyncGet(W)} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If null is passed in, the state value will remain unchanged.
     *
     * @param window The namespace for the state.
     * @param value The new value for the state.
     */
    public StateFuture<Void> asyncAdd(W window, RowData value) {
        windowState.setCurrentNamespace(window);
        return windowState.asyncAdd(value);
    }
}
