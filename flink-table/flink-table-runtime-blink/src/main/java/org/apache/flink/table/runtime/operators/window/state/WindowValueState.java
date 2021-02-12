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

package org.apache.flink.table.runtime.operators.window.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** A wrapper of {@link ValueState} which is easier to update based on window namespace. */
public final class WindowValueState<W> implements WindowState<W> {

    private final InternalValueState<RowData, W, RowData> windowState;

    public WindowValueState(InternalValueState<RowData, W, RowData> windowState) {
        this.windowState = windowState;
    }

    public void clear(W window) {
        windowState.setCurrentNamespace(window);
        windowState.clear();
    }

    /** Returns the current value for the state under current key and the given window. */
    public RowData value(W window) throws IOException {
        windowState.setCurrentNamespace(window);
        return windowState.value();
    }

    /**
     * Update the state with the given value under current key and the given window.
     *
     * @param window the window namespace.
     * @param value the new value for the state.
     */
    public void update(W window, RowData value) throws IOException {
        windowState.setCurrentNamespace(window);
        windowState.update(value);
    }
}
