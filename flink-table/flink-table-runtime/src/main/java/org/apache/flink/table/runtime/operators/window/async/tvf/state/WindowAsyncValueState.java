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

package org.apache.flink.table.runtime.operators.window.async.tvf.state;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** A wrapper of {@link ValueState} which is easier to update based on window namespace. */
public class WindowAsyncValueState<W> implements WindowAsyncState<W> {

    private final InternalValueState<RowData, W, RowData> windowState;

    public WindowAsyncValueState(InternalValueState<RowData, W, RowData> windowState) {
        this.windowState = windowState;
    }

    @Override
    public StateFuture<Void> asyncClear(W window) {
        windowState.setCurrentNamespace(window);
        return windowState.asyncClear();
    }

    /** Returns the current value for the state under current key and the given window. */
    public StateFuture<RowData> asyncValue(W window) throws IOException {
        windowState.setCurrentNamespace(window);
        return windowState.asyncValue();
    }

    /**
     * Update the state with the given value under current key and the given window.
     *
     * @param window the window namespace.
     * @param value the new value for the state.
     */
    public StateFuture<Void> asyncUpdate(W window, RowData value) throws IOException {
        windowState.setCurrentNamespace(window);
        return windowState.asyncUpdate(value);
    }
}
